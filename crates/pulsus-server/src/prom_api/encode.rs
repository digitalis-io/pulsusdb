//! The streaming JSON envelope encoder for `/api/v1/*` (docs/api.md §3).
//! Builds the response body incrementally from the already-materialized
//! `QueryResult`/`PlanExplain`/`Vec<String>`/discovery results — never a
//! `serde_json::Value` DOM over a result set (issue #32 architect plan,
//! mirroring `logs_api::encode`'s own contract).
//!
//! [`stream_array`] began as a copy of `logs_api::encode`'s own primitive;
//! since issue #795 its renderer appends to the chunk rather than returning
//! one (architect plan: `prom_api` is fully self-contained —
//! coders may be editing `logs_api/` concurrently, so extracting a shared
//! helper now would be a merge-conflict magnet), including the `.fuse()`
//! issue-#24 fix: the raw `unfold` stream is `.fuse()`d before
//! `Body::from_stream`, because `tower_http::compression::CompressionLayer`'s
//! gzip encoder polls the wrapped body once more past its final `None` to
//! observe EOF/flush — `Unfold`'s documented contract is that it must never
//! be polled again once it returns `Poll::Ready(None)`, and panics
//! otherwise. `Fuse` makes that extra poll a safe no-op.
//!
//! **Two formatters are Prometheus-exact, not Rust `Display`** — see each
//! function's own doc: [`prom_float`] reproduces Go
//! `strconv.FormatFloat(v,'f',-1,64)` (always fixed-point, **never**
//! scientific notation — a correction to the architect plan amendment §2's
//! pinned `'g'` mechanism, made against a live capture of real
//! `prom/prometheus:v3.13.0`; see [`prom_float`]'s own doc and this
//! issue's implementation notes); [`prom_timestamp`] trims trailing zeros
//! (diverging from `logs_api::encode`'s own `format_unix_seconds`, which
//! always pads `.fff` — a deliberate difference, not an oversight, because
//! Prometheus's own `jsonutil.MarshalTimestamp` trims).
//!
//! **Ordering (determinism):** every response shape here sorts its items by
//! label vector (vector/matrix: the label vector itself; series: the full
//! label-pair vector including the spliced `__name__`) before framing, so
//! wire output is deterministic and golden fixtures are byte-exact.

use axum::body::{Body, Bytes};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Serialize;

use pulsus_read::{
    ExemplarSeries, ExplainStage, HistMatrixSeries, HistOrFloat, HistVectorSample, MatrixSeries,
    MetricMeta, PlanExplain, QueryResult, TsdbStatus, VectorSample,
};

use crate::app::BuildInfo;

/// Builds a streaming JSON body: `prefix`, then `render(item, chunk)`
/// appending each item to its chunk (comma-separated), then `suffix`.
/// `items` (the already-materialized, domain data) is moved into the
/// stream's state and lives for the whole drain — only the *current* item's
/// chunk is additional, temporary encoder memory. See the module doc for the
/// `.fuse()` rationale (issue #24).
fn stream_array<T, R>(prefix: Vec<u8>, items: Vec<T>, render: R, suffix: Vec<u8>) -> Body
where
    T: Send + 'static,
    R: Fn(&T, &mut Vec<u8>) + Send + 'static,
{
    enum Step {
        Prefix,
        Item(usize),
        Suffix,
        Done,
    }

    struct State<T, R> {
        items: Vec<T>,
        render: R,
        step: Step,
        prefix: Vec<u8>,
        suffix: Vec<u8>,
    }

    let state = State {
        items,
        render,
        step: Step::Prefix,
        prefix,
        suffix,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        match state.step {
            Step::Prefix => {
                let bytes = std::mem::take(&mut state.prefix);
                state.step = if state.items.is_empty() {
                    Step::Suffix
                } else {
                    Step::Item(0)
                };
                Some((Ok::<_, std::io::Error>(Bytes::from(bytes)), state))
            }
            Step::Item(i) => {
                let mut chunk = Vec::new();
                if i > 0 {
                    chunk.push(b',');
                }
                (state.render)(&state.items[i], &mut chunk);
                let next = i + 1;
                state.step = if next < state.items.len() {
                    Step::Item(next)
                } else {
                    Step::Suffix
                };
                Some((Ok(Bytes::from(chunk)), state))
            }
            Step::Suffix => {
                let bytes = std::mem::take(&mut state.suffix);
                state.step = Step::Done;
                Some((Ok(Bytes::from(bytes)), state))
            }
            Step::Done => None,
        }
    });

    Body::from_stream(stream.fuse())
}

fn json_response(body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// [`json_string`] appended to `out` (issue #795), through the same
/// serializer.
fn push_json_string(out: &mut Vec<u8>, s: &str) {
    serde_json::to_writer(&mut *out, s).expect("a str always serialises");
}

fn labels_object_json(labels: &[(String, String)]) -> String {
    let mut out = Vec::new();
    push_labels_object(&mut out, labels);
    String::from_utf8(out).expect("labels are UTF-8")
}

/// The labels as a JSON object, appended to `out` (issue #795): keys in
/// byte order, and of a repeated key only its last value — the text a
/// `serde_json::Map` (a `BTreeMap` in this workspace, which does not enable
/// `preserve_order`) built from the same pairs gives.
fn push_labels_object(out: &mut Vec<u8>, labels: &[(String, String)]) {
    let mut refs: Vec<&(String, String)> = labels.iter().collect();
    refs.sort_by(|a, b| a.0.cmp(&b.0));
    out.push(b'{');
    let mut first = true;
    for (i, (k, v)) in refs.iter().map(|kv| (&kv.0, &kv.1)).enumerate() {
        if refs.get(i + 1).is_some_and(|next| next.0 == *k) {
            continue;
        }
        if !first {
            out.push(b',');
        }
        first = false;
        push_json_string(out, k);
        out.push(b':');
        push_json_string(out, v);
    }
    out.push(b'}');
}

// ---------------------------------------------------------------------
// Prometheus-exact formatters (architect plan amendment §2 — the wire-
// compat crux; see this module's doc comment).
// ---------------------------------------------------------------------

/// **Correction to the architect plan amendment §2 (deviation, see the
/// implementation notes posted on issue #32):** the amendment pinned
/// `strconv.FormatFloat(v,'g',-1,64)` (an `%e`/`%f` threshold at `exp < -4
/// || exp >= 21`). A live capture against real `prom/prometheus:v3.13.0`
/// (`crates/pulsus-server/tests/fixtures/prom_api/`, this issue's own
/// golden-capture mandate) proved that assumption **wrong**: Prometheus's
/// actual API float formatting is `strconv.FormatFloat(v,'f',-1,64)` —
/// plain fixed-point decimal, unconditionally, with **no** scientific
/// notation at any magnitude. Verified directly against the running
/// capture server: `1e21` -> `"1000000000000000000000"` (not `"1e+21"`),
/// `5e-324` -> a ~323-zero fixed-point fraction (not `"5e-324"`), and
/// `1.7976931348623157e308` (`f64::MAX`) -> the full ~309-digit integer.
/// This function therefore always renders `%f`-style via
/// [`push_prom_float`]'s fixed-point step — never scientific notation —
/// reproducing Go's shortest round-trip decimal digits (via
/// `format!("{v:e}")`'s mantissa) placed at the correct decimal-point
/// position for *every* finite magnitude, including the `5e-324`
/// subnormal boundary. Special values first (`NaN`/`+Inf`/
/// `-Inf` as literal strings, `-0`/`0` verbatim). Sample values are always
/// emitted as **quoted** JSON strings by the caller (Prometheus
/// convention), never bare numbers — this function itself returns the
/// unquoted text.
///
/// Since issue #795 a test wrapper over [`push_prom_float`], which every
/// writer calls.
#[cfg(test)]
pub(crate) fn prom_float(v: f64) -> String {
    let mut out = Vec::new();
    push_prom_float(&mut out, v);
    String::from_utf8(out).expect("ASCII")
}

/// [`prom_float`] appended to `out` (issue #795).
pub(crate) fn push_prom_float(out: &mut Vec<u8>, v: f64) {
    if v.is_nan() {
        out.extend_from_slice(b"NaN");
        return;
    }
    if v.is_infinite() {
        out.extend_from_slice(if v.is_sign_positive() {
            b"+Inf"
        } else {
            b"-Inf"
        });
        return;
    }
    if v == 0.0 {
        out.extend_from_slice(if v.is_sign_negative() { b"-0" } else { b"0" });
        return;
    }

    // The longest f64 `{:e}` text is 24 bytes (`-2.2250738585072014e-308`).
    let mut buf = [0u8; 32];
    let len = {
        let mut cursor: &mut [u8] = &mut buf;
        std::io::Write::write_fmt(&mut cursor, format_args!("{v:e}"))
            .expect("an f64's {v:e} text fits 32 bytes");
        32 - cursor.len()
    };
    let sci = &buf[..len];
    let e = sci
        .iter()
        .position(|&b| b == b'e')
        .expect("Rust's {v:e} formatter always emits exactly one 'e'");
    let (mantissa_part, exp_part) = (&sci[..e], &sci[e + 1..]);
    let exp: i32 = std::str::from_utf8(exp_part)
        .expect("ASCII")
        .parse()
        .expect("Rust's {v:e} exponent is always a valid integer literal");

    let mut digits = [0u8; 32];
    let mut n = 0;
    for &b in mantissa_part {
        if b != b'-' && b != b'.' {
            digits[n] = b;
            n += 1;
        }
    }
    let digits = &digits[..n];
    if mantissa_part.first() == Some(&b'-') {
        out.push(b'-');
    }

    // Go `%f`-style rendering: the decimal point sits `exp + 1` digits from
    // the left of `digits` (Rust's shortest-round-trip mantissa never
    // carries a trailing zero, so no trailing-zero trim is ever needed
    // here).
    let point = exp + 1;
    if point <= 0 {
        out.extend_from_slice(b"0.");
        out.resize(out.len() + (-point) as usize, b'0');
        out.extend_from_slice(digits);
    } else if (point as usize) >= digits.len() {
        out.extend_from_slice(digits);
        out.resize(out.len() + point as usize - digits.len(), b'0');
    } else {
        let (whole, frac) = digits.split_at(point as usize);
        out.extend_from_slice(whole);
        out.push(b'.');
        out.extend_from_slice(frac);
    }
}

/// Prometheus-style timestamp (`jsonutil.MarshalTimestamp`): integer
/// seconds when `ms` lands exactly on a second boundary, else
/// `secs.fff`-with-trailing-zeros-trimmed — deliberately diverges from
/// `logs_api::encode::format_unix_seconds`'s always-`.fff` padding (see
/// the module doc). Emitted as a **bare** JSON number by the caller, never
/// a quoted string.
pub(crate) fn prom_timestamp(ms: i64) -> String {
    let mut out = Vec::new();
    push_prom_timestamp(&mut out, ms);
    String::from_utf8(out).expect("ASCII")
}

/// [`prom_timestamp`] appended to `out` (issue #795): the trimmed
/// `{millis:03}` written digit by digit.
fn push_prom_timestamp(out: &mut Vec<u8>, ms: i64) {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    std::io::Write::write_fmt(out, format_args!("{secs}")).expect("a Vec never fails a write");
    if millis != 0 {
        let (hundreds, tens, ones) = (millis / 100, millis / 10 % 10, millis % 10);
        out.push(b'.');
        out.push(b'0' + hundreds as u8);
        if tens != 0 || ones != 0 {
            out.push(b'0' + tens as u8);
        }
        if ones != 0 {
            out.push(b'0' + ones as u8);
        }
    }
}

/// Sample values are always quoted JSON strings (docs/api.md §3.1).
fn quoted_value(v: f64) -> String {
    let mut out = Vec::new();
    push_quoted_value(&mut out, v);
    String::from_utf8(out).expect("ASCII")
}

/// [`quoted_value`] appended to `out` (issue #795). The text is only
/// digits, `.`, `-`, `+Inf`, `-Inf` and `NaN`, which JSON escaping leaves
/// alone.
fn push_quoted_value(out: &mut Vec<u8>, v: f64) {
    out.push(b'"');
    push_prom_float(out, v);
    out.push(b'"');
}

/// The error envelope `query_response`'s unreachable `QueryResult::Streams`
/// arm builds — mirrors `error::ApiError`'s 3-field shape, kept local to
/// this module since it never flows through `ApiError` itself.
#[derive(Serialize)]
struct UnreachableStreamsError {
    status: &'static str,
    #[serde(rename = "errorType")]
    error_type: &'static str,
    error: &'static str,
}

// ---------------------------------------------------------------------
// Explain (issue #32: adds `exactness`; issue #18: `"rollup"` when a rollup
// level served part of a selector, a selector's range was rounded, or the
// query's steps take the value of the bucket that holds them, else
// `"raw-exact"`).
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct ExplainWire<'a> {
    result_type: &'a str,
    exactness: &'static str,
    stages: Vec<StageWire<'a>>,
    /// Issue #492: the compiled plan's shape, an ADDITIVE sibling key.
    /// Skipped when absent, which it is on every path today.
    #[serde(skip_serializing_if = "Option::is_none")]
    plan: Option<&'a pulsus_read::compile::plan::PlanShape>,
    /// Issue #548: one compiled plan per selector-list entry that reads.
    /// **Skipped when empty**, which it is on every route but this one
    /// and on every request here without the header — so a response that
    /// carries no plan is byte-identical to one from before the key
    /// existed.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    plans: &'a [pulsus_read::compile::plan::PlanShape],
}

#[derive(Serialize)]
struct StageWire<'a> {
    name: &'a str,
    sql: &'a str,
    note: Option<&'a str>,
}

fn explain_json(e: &PlanExplain) -> String {
    let wire = ExplainWire {
        result_type: e.result_type,
        exactness: if e.rollup { "rollup" } else { "raw-exact" },
        stages: e
            .stages
            .iter()
            .map(|s: &ExplainStage| StageWire {
                name: s.name,
                sql: &s.sql,
                note: s.note.as_deref(),
            })
            .collect(),
        plan: e.plan.as_ref(),
        plans: &e.plans,
    };
    serde_json::to_string(&wire).unwrap_or_else(|_| "{}".to_string())
}

fn explain_suffix(mut suffix: String, explain: Option<&PlanExplain>) -> String {
    if let Some(e) = explain {
        suffix.push_str(",\"explain\":");
        suffix.push_str(&explain_json(e));
    }
    suffix
}

// ---------------------------------------------------------------------
// query / query_range (issue #32; explain only on these two endpoints,
// per the architect plan amendment).
// ---------------------------------------------------------------------

fn push_matrix_item(s: &MatrixSeries, out: &mut Vec<u8>) {
    out.reserve(64 + 32 * s.points.len());
    out.extend_from_slice(b"{\"metric\":");
    push_labels_object(out, &s.labels);
    out.extend_from_slice(b",\"values\":[");
    for (i, (t_ms, value)) in s.points.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'[');
        push_prom_timestamp(out, *t_ms);
        out.push(b',');
        push_quoted_value(out, *value);
        out.push(b']');
    }
    out.extend_from_slice(b"]}");
}

fn push_vector_item(s: &VectorSample, at_ms: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(b"{\"metric\":");
    push_labels_object(out, &s.labels);
    out.extend_from_slice(b",\"value\":[");
    push_prom_timestamp(out, at_ms);
    out.push(b',');
    push_quoted_value(out, s.value);
    out.extend_from_slice(b"]}");
}

/// M7-A5b-i: the boundary-inclusivity code upstream's `MarshalHistogram`
/// writes as a bucket array's 1st element (`util/jsonutil/marshal.go`,
/// pinned `40af9c2`): `0` lower-exclusive/upper-inclusive, `1` lower-
/// inclusive/upper-exclusive, `2` both exclusive (the default — an open
/// interval), `3` both inclusive.
fn histogram_boundary_code(lower_inclusive: bool, upper_inclusive: bool) -> u8 {
    match (lower_inclusive, upper_inclusive) {
        (true, true) => 3,
        (true, false) => 1,
        (false, true) => 0,
        (false, false) => 2,
    }
}

/// Renders a native histogram to Prometheus's `MarshalHistogram` wire shape
/// (`util/jsonutil/marshal.go`, pinned `40af9c2`):
/// `{"count":"<c>","sum":"<s>","buckets":[[<code>,"<lower>","<upper>","<count>"],…]}`
/// — `count`/`sum`/bucket fields are quoted float strings (`quoted_value`,
/// [`prom_float`]'s own contract — bit-for-bit `MarshalFloat`); zero-count
/// buckets are skipped; the `buckets` key is omitted entirely when every
/// bucket is empty (upstream's `bucketFound` guard).
fn push_histogram_json(out: &mut Vec<u8>, h: &pulsus_model::FloatHistogram) {
    out.extend_from_slice(b"{\"count\":");
    push_quoted_value(out, h.count);
    out.extend_from_slice(b",\"sum\":");
    push_quoted_value(out, h.sum);
    let mut any = false;
    for b in h.all_buckets() {
        if b.count == 0.0 {
            continue;
        }
        out.extend_from_slice(if any { b"," } else { b",\"buckets\":[" });
        any = true;
        let code = histogram_boundary_code(b.lower_inclusive, b.upper_inclusive);
        out.push(b'[');
        out.push(b'0' + code);
        out.push(b',');
        push_quoted_value(out, b.lower);
        out.push(b',');
        push_quoted_value(out, b.upper);
        out.push(b',');
        push_quoted_value(out, b.count);
        out.push(b']');
    }
    if any {
        out.push(b']');
    }
    out.push(b'}');
}

fn push_hist_or_float(out: &mut Vec<u8>, v: &HistOrFloat) {
    match v {
        HistOrFloat::Float(f) => push_quoted_value(out, *f),
        HistOrFloat::Hist(h) => push_histogram_json(out, h),
    }
}

/// M7-A5b-i: one `QueryResult::VectorHist` element — `"value"` for a float,
/// `"histogram"` for a native histogram (`web/api/v1/json_codec.go`
/// `marshalSampleJSON`, pinned `40af9c2`).
fn push_hist_vector_item(s: &HistVectorSample, at_ms: i64, out: &mut Vec<u8>) {
    let key: &[u8] = match &s.value {
        HistOrFloat::Float(_) => b"value",
        HistOrFloat::Hist(_) => b"histogram",
    };
    out.extend_from_slice(b"{\"metric\":");
    push_labels_object(out, &s.labels);
    out.extend_from_slice(b",\"");
    out.extend_from_slice(key);
    out.extend_from_slice(b"\":[");
    push_prom_timestamp(out, at_ms);
    out.push(b',');
    push_hist_or_float(out, &s.value);
    out.extend_from_slice(b"]}");
}

/// M7-A5b-i: one `QueryResult::MatrixHist` series — upstream splits float
/// and histogram points into SEPARATE `"values"`/`"histograms"` arrays
/// (`promql.Series{Floats,Histograms}`, `marshalSeriesJSON`, pinned
/// `40af9c2`), each internally time-ordered but not interleaved with the
/// other; both keys are omitted when empty (mirrors the float `"values"`-
/// only shape when there are no histogram points at all).
fn push_hist_matrix_item(s: &HistMatrixSeries, out: &mut Vec<u8>) {
    let mut values: Vec<u8> = Vec::new();
    let mut histograms: Vec<u8> = Vec::new();
    for (t_ms, v) in &s.points {
        let array = match v {
            HistOrFloat::Float(_) => &mut values,
            HistOrFloat::Hist(_) => &mut histograms,
        };
        if !array.is_empty() {
            array.push(b',');
        }
        array.push(b'[');
        push_prom_timestamp(array, *t_ms);
        array.push(b',');
        push_hist_or_float(array, v);
        array.push(b']');
    }
    out.extend_from_slice(b"{\"metric\":");
    push_labels_object(out, &s.labels);
    if !values.is_empty() {
        out.extend_from_slice(b",\"values\":[");
        out.extend_from_slice(&values);
        out.push(b']');
    }
    if !histograms.is_empty() {
        out.extend_from_slice(b",\"histograms\":[");
        out.extend_from_slice(&histograms);
        out.push(b']');
    }
    out.push(b'}');
}

/// M7-A5b-i: the `warnings`/`infos` envelope suffix — TOP-LEVEL siblings of
/// `data` (`web/api/v1/api.go` `Response`, pinned `40af9c2`: `Warnings
/// []string \`json:"warnings,omitempty"\``, `Infos` likewise), so this is
/// appended AFTER `data`'s own closing brace, never nested inside it (unlike
/// `explain_suffix`). Empty ⇒ no bytes ⇒ float-only responses (empty
/// `Annotations`) stay byte-identical. Caps/dedup/overflow-line contract:
/// [`pulsus_promql::Annotations::as_strings`] (upstream `AsStrings(query, 10,
/// 10)`, `web/api/v1/api.go`). `query` is the SAME raw query text the
/// handler parsed (issue #128): each kept line ends with the byte-exact
/// `" (<line>:<col>)"` source-position suffix upstream renders there.
///
/// `stats_requested` (issue #499) appends [`STATS_UNSUPPORTED_WARNING`]
/// AFTER the cap and its overflow line: it is our own notice, not one of
/// the evaluation's, so the cap can never drop it.
fn annotations_suffix(
    query: &str,
    annotations: &pulsus_promql::Annotations,
    stats_requested: bool,
) -> String {
    let (mut warnings, infos) = annotations.as_strings(query, 10, 10);
    if stats_requested {
        warnings.push(STATS_UNSUPPORTED_WARNING.to_string());
    }
    let mut s = String::new();
    if !warnings.is_empty() {
        s.push_str(",\"warnings\":[");
        for (i, w) in warnings.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&json_string(w));
        }
        s.push(']');
    }
    if !infos.is_empty() {
        s.push_str(",\"infos\":[");
        for (i, w) in infos.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&json_string(w));
        }
        s.push(']');
    }
    s
}

/// Encodes a `query`/`query_range` result (docs/api.md §3.1/§3.2):
/// `data.resultType`/`result`(/`explain`). `at_ms` is the instant
/// evaluation time (`/query`'s `time` param) — only read for
/// [`QueryResult::Vector`]/[`QueryResult::Scalar`] (never produced by a
/// range query); `query_range` callers may pass any placeholder.
///
/// `ordered` (issue #68, M6-05 — task-manager-adjudicated shape): `true`
/// skips the vector arm's deterministic label re-sort so the evaluator's
/// own order survives on the wire — set ONLY for a sort-rooted
/// (`pulsus_promql::expr_is_sort_root`) **instant** query by
/// `handlers::run_query`; every other query (and every matrix result,
/// which has no upstream ordering contract here) keeps the label-sorted
/// output the M2 determinism discipline pinned. Without this, `sort()`/
/// `sort_desc()` would be inert at the API.
// Test-only since issue #182: the traces metrics endpoints (its last
// production caller) now emit the Tempo-native body
// (`traces_api::metrics_response`); the Prometheus endpoints call
// `query_response_annotated` directly. Kept as a thin test convenience.
#[cfg(test)]
pub(crate) fn query_response(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ms: i64,
    ordered: bool,
) -> Response {
    let empty = pulsus_promql::Annotations::new();
    // Empty annotations render zero bytes regardless of the query text,
    // so the float-only path needs no query threading (issue #128).
    query_response_annotated(
        result,
        explain,
        at_ms,
        "",
        empty,
        ResponseShape {
            ordered,
            limit: None,
            stats_requested: false,
        },
    )
}

/// Keeps the first `n` of `items`; `true` when that dropped some.
fn cut_to<T>(items: &mut Vec<T>, n: usize) -> bool {
    let cut = items.len() > n;
    items.truncate(n);
    cut
}

/// How a `query`/`query_range` answer is shaped beyond its result (issue
/// #499): `ordered` keeps the evaluator's vector order (a sort-rooted
/// instant query), `limit` truncates the series, and `stats_requested`
/// adds the notice that no statistics are returned.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponseShape {
    pub(crate) ordered: bool,
    pub(crate) limit: Option<usize>,
    pub(crate) stats_requested: bool,
}

/// `query_response` plus the `warnings`/`infos` envelope arrays (M7-A5b-i)
/// and the two histogram-carrying `QueryResult` variants
/// ([`QueryResult::VectorHist`]/[`QueryResult::MatrixHist`], replacing the
/// A5a `HistogramResultUnsupported` reject). Split from `query_response` so
/// every pre-existing float-only call site (and its golden byte-exact
/// tests) is untouched — an empty [`pulsus_promql::Annotations`] renders
/// zero extra bytes (AC6, `omitempty`).
pub(crate) fn query_response_annotated(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ms: i64,
    query: &str,
    annotations: pulsus_promql::Annotations,
    shape: ResponseShape,
) -> Response {
    let ordered = shape.ordered;
    // Issue #499: `limit` cuts what the evaluator returned, as the
    // reference does — an instant vector in the evaluator's order, before
    // the presentation sort; a matrix after it, since the reference's
    // matrix is already sorted by labels — and says so in `warnings`.
    let mut result = result;
    let cut = match (&mut result, shape.limit) {
        (QueryResult::Vector(items), Some(n)) => cut_to(items, n),
        (QueryResult::VectorHist(items), Some(n)) => cut_to(items, n),
        (QueryResult::Matrix(items), Some(n)) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            cut_to(items, n)
        }
        (QueryResult::MatrixHist(items), Some(n)) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            cut_to(items, n)
        }
        _ => false,
    };
    let mut annotations = annotations;
    if cut {
        annotations.plain_warning(TRUNCATION_WARNING);
    }
    let annos = annotations_suffix(query, &annotations, shape.stats_requested);
    match result {
        QueryResult::Vector(mut items) => {
            if !ordered {
                items.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}{annos}}}").into_bytes();
            json_response(stream_array(
                prefix,
                items,
                move |s: &VectorSample, out: &mut Vec<u8>| push_vector_item(s, at_ms, out),
                suffix,
            ))
        }
        QueryResult::Matrix(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}{annos}}}").into_bytes();
            json_response(stream_array(prefix, items, push_matrix_item, suffix))
        }
        // M7-A5b-i: a histogram-valued instant result — same shape as the
        // `Vector` arm above, one level of indirection through
        // `push_hist_vector_item` for the per-element `"value"`/
        // `"histogram"` key choice.
        QueryResult::VectorHist(mut items) => {
            if !ordered {
                items.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}{annos}}}").into_bytes();
            json_response(stream_array(
                prefix,
                items,
                move |s: &HistVectorSample, out: &mut Vec<u8>| push_hist_vector_item(s, at_ms, out),
                suffix,
            ))
        }
        // M7-A5b-i: a histogram-valued range result.
        QueryResult::MatrixHist(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}{annos}}}").into_bytes();
            json_response(stream_array(prefix, items, push_hist_matrix_item, suffix))
        }
        // Code-review round-1 fix: `explain` must nest under `data`
        // exactly like the vector/matrix arms above — the data object is
        // kept **open** (no closing `}` yet) until after
        // `explain_suffix`, which appends `,"explain":{...}` while still
        // inside `data`; only then do both closing braces (`data` then
        // the root object) get appended. The prior version closed `data`
        // before calling `explain_suffix`, which put `explain` as a
        // top-level sibling of `data` instead of nested under it.
        QueryResult::Scalar(v) => {
            let data_body = explain_suffix(
                format!(
                    "{{\"status\":\"success\",\"data\":{{\"resultType\":\"scalar\",\"result\":[{},{}]",
                    prom_timestamp(at_ms),
                    quoted_value(v)
                ),
                explain.as_ref(),
            );
            json_response(Body::from(format!("{data_body}}}{annos}}}")))
        }
        // Issue #86 (M6-08d): a top-level string-literal query — the
        // Prometheus `resultType:"string"` shape, `result: [<t>,"<val>"]`
        // with the request's evaluation time stamped here (the Scalar
        // precedent; the value variant carries no timestamp).
        QueryResult::String(s) => {
            let data_body = explain_suffix(
                format!(
                    "{{\"status\":\"success\",\"data\":{{\"resultType\":\"string\",\"result\":[{},{}]",
                    prom_timestamp(at_ms),
                    json_string(&s)
                ),
                explain.as_ref(),
            );
            json_response(Body::from(format!("{data_body}}}{annos}}}")))
        }
        // Unreachable: `MetricsEngine` never produces `QueryResult::Streams`
        // (a `LogQlEngine`-only variant of the shared `QueryResult` type,
        // mirrors `logs_api::encode`'s own historical handling of
        // `QueryResult::Scalar` before issue #31 landed). Kept as a
        // well-formed error response rather than a panic — never trust an
        // upstream invariant to hold forever in a shared enum.
        QueryResult::Streams { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(UnreachableStreamsError {
                status: "error",
                error_type: "internal",
                error: "unexpected streams result from a metrics query",
            }),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------
// labels / label_values / series (issue #32; no explain — architect plan
// amendment: "emitted ... on the two query endpoints only").
// ---------------------------------------------------------------------

/// The truncation warning for the three discovery endpoints (issue #471
/// M4) and for `/query` and `/query_range` (issue #499) — the reference's
/// own string, byte-for-byte.
pub(crate) const TRUNCATION_WARNING: &str = "results truncated due to limit";

/// The notice a non-empty `stats` parameter adds to `warnings` (issue
/// #499): no query statistics are returned.
pub(crate) const STATS_UNSUPPORTED_WARNING: &str =
    "parameter \"stats\" is not supported; no query statistics are returned";

/// The `]}`/`],"warnings":[...]}` suffix for the discovery encoders (issue
/// #471 M4). `None` produces today's bytes exactly, so every existing body
/// assertion stays green; `Some` appends `warnings` as a **top-level
/// sibling of `data`** — the position the client looks in, and the one
/// [`annotations_suffix`] already uses on the query path.
fn array_suffix(warning: Option<&'static str>) -> Vec<u8> {
    match warning {
        None => b"]}".to_vec(),
        Some(w) => format!("],\"warnings\":[{}]}}", json_string(w)).into_bytes(),
    }
}

/// Encodes a `labels`/`label/{name}/values` result: `{"status":"success",
/// "data":["name1",...]}`, plus a top-level `warnings` array when the
/// discovery `limit` actually truncated the result (issue #471 M4).
pub(crate) fn string_array_response(items: Vec<String>, warning: Option<&'static str>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    json_response(stream_array(
        prefix,
        items,
        |s: &String, out: &mut Vec<u8>| out.extend_from_slice(json_string(s).as_bytes()),
        array_suffix(warning),
    ))
}

/// Encodes a `series` result: `{"status":"success","data":[{k:v...},...]}`
/// — each item is a full label-pair vector, `__name__` already spliced in
/// by `MetricsEngine::series`. `warning` as in [`string_array_response`].
pub(crate) fn series_response(
    items: Vec<Vec<(String, String)>>,
    warning: Option<&'static str>,
) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    json_response(stream_array(
        prefix,
        items,
        |pairs: &Vec<(String, String)>, out: &mut Vec<u8>| {
            out.extend_from_slice(labels_object_json(pairs).as_bytes())
        },
        array_suffix(warning),
    ))
}

// ---------------------------------------------------------------------
// metadata (issues #32 and #500) — Prometheus's own shape: a map keyed by
// metric name, each value an array of that name's descriptor objects, one
// per distinct descriptor `metric_metadata` holds.
// ---------------------------------------------------------------------

fn render_metadata_descriptor(m: &MetricMeta) -> String {
    format!(
        "{{\"type\":{},\"help\":{},\"unit\":{}}}",
        json_string(&m.metric_type),
        json_string(&m.help),
        json_string(&m.unit)
    )
}

/// One name's key and its array of descriptors.
fn render_metadata_entry(group: &[MetricMeta]) -> Vec<u8> {
    let descriptors: Vec<String> = group.iter().map(render_metadata_descriptor).collect();
    format!(
        "{}:[{}]",
        json_string(&group[0].name),
        descriptors.join(",")
    )
    .into_bytes()
}

/// One key per name, whose array holds that name's entries in row order.
/// The names are sorted; a stable sort keeps each name's entries in the
/// order the rows arrived.
pub(crate) fn metadata_response(mut items: Vec<MetricMeta>) -> Response {
    items.sort_by(|a, b| a.name.cmp(&b.name));
    let mut groups: Vec<Vec<MetricMeta>> = Vec::new();
    for item in items {
        match groups.last_mut() {
            Some(group) if group[0].name == item.name => group.push(item),
            _ => groups.push(vec![item]),
        }
    }
    let prefix = b"{\"status\":\"success\",\"data\":{".to_vec();
    let suffix = b"}}".to_vec();
    json_response(stream_array(
        prefix,
        groups,
        |group: &Vec<MetricMeta>, out: &mut Vec<u8>| {
            out.extend_from_slice(&render_metadata_entry(group))
        },
        suffix,
    ))
}

// ---------------------------------------------------------------------
// query_exemplars (issue #501 part 2).
// ---------------------------------------------------------------------

/// `/api/v1/query_exemplars`' answer (issue #501 part 2): `None` — a
/// query with no selector — is `{"status":"success"}` with no `data`, as
/// the reference's envelope omits a nil result; otherwise one object per
/// series, `seriesLabels` then `exemplars`, each exemplar `labels`,
/// `value` (the samples' formatter, quoted) and `timestamp`
/// (`web/api/v1/json_codec.go`, `marshalExemplarJSON`).
pub(crate) fn query_exemplars_response(answer: Option<Vec<ExemplarSeries>>) -> Response {
    let Some(series) = answer else {
        return json_response(Body::from(r#"{"status":"success"}"#));
    };
    json_response(stream_array(
        b"{\"status\":\"success\",\"data\":[".to_vec(),
        series,
        |s: &ExemplarSeries, out: &mut Vec<u8>| out.extend_from_slice(&render_exemplar_series(s)),
        b"]}".to_vec(),
    ))
}

fn render_exemplar_series(series: &ExemplarSeries) -> Vec<u8> {
    let mut out = format!(
        "{{\"seriesLabels\":{},\"exemplars\":[",
        labels_object_json(&series.series_labels)
    );
    for (i, e) in series.exemplars.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"labels\":{},\"value\":{},\"timestamp\":{}}}",
            labels_object_json(&e.labels),
            quoted_value(e.value),
            prom_timestamp(e.unix_milli)
        ));
    }
    out.push_str("]}");
    out.into_bytes()
}

// ---------------------------------------------------------------------
// status/* (issue #32) — small, fixed-size envelopes; `serde_json` is fine
// here, mirroring `logs_api::encode`'s own "never the potentially large
// result array" carve-out.
// ---------------------------------------------------------------------

/// The `{"status":"success","data":...}` envelope for every `status/*`
/// response — a typed struct, not the `serde_json::json!` macro: `Value`'s
/// default `Map` is a `BTreeMap` (this workspace does not enable serde_json's
/// `preserve_order` feature), so a `json!({"status":...,"data":...})`
/// literal silently re-sorts to alphabetical key order (`data` before
/// `status`) at serialization time — wrong wire order for a byte-exact
/// golden. A `#[derive(Serialize)]` struct's field order is the struct's
/// declaration order, always.
#[derive(Serialize)]
struct SuccessEnvelope<T> {
    status: &'static str,
    data: T,
}

fn success<T: Serialize>(data: T) -> Response {
    axum::Json(SuccessEnvelope {
        status: "success",
        data,
    })
    .into_response()
}

#[derive(Serialize)]
struct BuildInfoWire<'a> {
    version: &'a str,
    revision: &'a str,
    branch: &'a str,
    #[serde(rename = "buildUser")]
    build_user: &'a str,
    #[serde(rename = "buildDate")]
    build_date: &'a str,
    #[serde(rename = "goVersion")]
    go_version: &'a str,
}

pub(crate) fn status_buildinfo_response(build: &BuildInfo) -> Response {
    success(BuildInfoWire {
        version: &build.version,
        revision: &build.revision,
        branch: "",
        build_user: "",
        build_date: &build.built_at,
        go_version: &build.rustc,
    })
}

#[derive(Serialize)]
struct ConfigWire<'a> {
    yaml: &'a str,
}

pub(crate) fn status_config_response(yaml: &str) -> Response {
    success(ConfigWire { yaml })
}

pub(crate) fn status_flags_response() -> Response {
    success(serde_json::Map::new())
}

#[derive(Serialize)]
struct RuntimeInfoWire {
    #[serde(rename = "startTime")]
    start_time: String,
    #[serde(rename = "storageRetention")]
    storage_retention: String,
    /// Issue #18 part B: the rollup levels the database holds, smallest
    /// first, as durations; `[]` when none or not yet read.
    #[serde(rename = "rollupLevels")]
    rollup_levels: Vec<String>,
}

pub(crate) fn status_runtimeinfo_response(
    start_time_rfc3339: String,
    retention_days: u32,
    rollup_levels: Vec<String>,
) -> Response {
    success(RuntimeInfoWire {
        start_time: start_time_rfc3339,
        storage_retention: format!("{retention_days}d"),
        rollup_levels,
    })
}

/// Code-review round-1 fix: `numSamples` was dropped — never a real
/// Prometheus `headStats` field, and serving it required a live
/// ClickHouse `count()`, violating `status/tsdb`'s zero-ClickHouse
/// contract (docs/api.md §3.4).
#[derive(Serialize)]
struct HeadStatsWire {
    #[serde(rename = "numSeries")]
    num_series: u64,
}

#[derive(Serialize)]
struct MetricCardinalityWire {
    name: String,
    value: u64,
}

#[derive(Serialize)]
struct TsdbStatusWire {
    #[serde(rename = "headStats")]
    head_stats: HeadStatsWire,
    #[serde(rename = "seriesCountByMetricName")]
    series_count_by_metric_name: Vec<MetricCardinalityWire>,
}

pub(crate) fn status_tsdb_response(status: TsdbStatus) -> Response {
    success(TsdbStatusWire {
        head_stats: HeadStatsWire {
            num_series: status.num_series,
        },
        series_count_by_metric_name: status
            .series_count_by_metric_name
            .into_iter()
            .map(|(name, value)| MetricCardinalityWire { name, value })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_read::StoredExemplar;

    /// No limit, no stats, the presentation sort.
    const NO_SHAPE: ResponseShape = ResponseShape {
        ordered: false,
        limit: None,
        stats_requested: false,
    };

    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::routing::get;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tower::ServiceExt;

    async fn body_string(res: Response) -> String {
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    // --- prom_float goldens ---
    //
    // Every non-trivial case below is asserted against the **literal
    // string captured from a live `prom/prometheus:v3.13.0` instance**
    // (`crates/pulsus-server/tests/fixtures/prom_api/query.float_goldens.json`
    // — capture.sh's `float_goldens` case; see that fixture and its
    // PROVENANCE.md entry), not hand-derived — this is the deviation from
    // the architect plan amendment §2 documented in this issue's
    // implementation notes: real Prometheus formats every finite value
    // via Go `strconv.FormatFloat(v,'f',-1,64)` (always fixed-point),
    // never `'g'`/scientific notation at any magnitude.

    #[test]
    fn prom_float_special_values() {
        assert_eq!(prom_float(f64::NAN), "NaN");
        assert_eq!(prom_float(f64::INFINITY), "+Inf");
        assert_eq!(prom_float(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn prom_float_zero_and_negative_zero() {
        assert_eq!(prom_float(0.0), "0");
        assert_eq!(prom_float(-0.0), "-0");
    }

    #[test]
    fn prom_float_small_integers() {
        assert_eq!(prom_float(1.0), "1");
        assert_eq!(prom_float(-1.0), "-1");
        assert_eq!(prom_float(1.5), "1.5");
        assert_eq!(prom_float(42.0), "42");
    }

    #[test]
    fn prom_float_100000_stays_fixed_form() {
        assert_eq!(prom_float(100_000.0), "100000");
    }

    #[test]
    fn prom_float_1e20_is_the_full_fixed_point_integer() {
        assert_eq!(prom_float(1e20), "100000000000000000000");
    }

    #[test]
    fn prom_float_1e21_is_the_full_fixed_point_integer_never_scientific() {
        // Captured golden: real Prometheus v3.13.0 never switches to `%e`.
        assert_eq!(prom_float(1e21), "1000000000000000000000");
    }

    #[test]
    fn prom_float_1e_minus_4_stays_fixed_form() {
        assert_eq!(prom_float(1e-4), "0.0001");
    }

    #[test]
    fn prom_float_1e_minus_5_stays_fixed_form_never_scientific() {
        // Captured golden.
        assert_eq!(prom_float(1e-5), "0.00001");
    }

    #[test]
    fn prom_float_subnormal_5e_minus_324() {
        // Captured golden: a 323-zero fixed-point fraction, not "5e-324".
        let expected = format!("0.{}5", "0".repeat(323));
        assert_eq!(prom_float(5e-324), expected);
    }

    #[test]
    fn prom_float_f64_max() {
        // Captured golden: the full ~309-digit fixed-point integer.
        let expected = "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(prom_float(f64::MAX), expected);
    }

    #[test]
    fn prom_float_negative_small_magnitude_stays_fixed_form() {
        // Captured golden.
        let expected = format!("-0.{}1", "0".repeat(299));
        assert_eq!(prom_float(-1e-300), expected);
    }

    #[test]
    fn prom_float_negative_fixed() {
        assert_eq!(prom_float(-100_000.0), "-100000");
    }

    #[test]
    fn prom_float_a_rate_shaped_fractional() {
        assert_eq!(
            prom_float(0.016_666_666_666_666_666),
            "0.016666666666666666"
        );
    }

    // --- prom_timestamp goldens (architect plan amendment §2) ---

    #[test]
    fn prom_timestamp_whole_seconds_has_no_fraction() {
        assert_eq!(prom_timestamp(1_435_781_451_000), "1435781451");
    }

    #[test]
    fn prom_timestamp_one_decimal_place() {
        assert_eq!(prom_timestamp(1_435_781_451_500), "1435781451.5");
    }

    #[test]
    fn prom_timestamp_three_decimal_places() {
        assert_eq!(prom_timestamp(1_435_781_451_781), "1435781451.781");
    }

    #[test]
    fn prom_timestamp_at_zero() {
        assert_eq!(prom_timestamp(0), "0");
    }

    #[test]
    fn prom_timestamp_a_negative_whole_second_has_no_fraction() {
        // Guards the negative-epoch bucket label the trace-metrics Int64-ms
        // fix (issue #59 re-audit) newly enables: a pre-1970 whole-second
        // bucket, e.g. 1969-12-31T23:00:00Z. `div_euclid`/`rem_euclid` are
        // correct for this whole-second case (unaffected — trace-metrics
        // buckets are always whole-second aligned).
        assert_eq!(prom_timestamp(-3_600_000), "-3600");
    }

    // --- query_response wire shapes ---

    #[tokio::test]
    async fn vector_envelope_is_byte_exact_for_a_single_sample() {
        let sample = VectorSample {
            labels: vec![("job".to_string(), "api".to_string())],
            value: 42.0,
        };
        let res = query_response(QueryResult::Vector(vec![sample]), None, 5_500, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[5.5,"42"]}]}}"#
        );
    }

    #[tokio::test]
    async fn matrix_envelope_is_byte_exact_for_a_single_series() {
        let series = MatrixSeries {
            labels: vec![("job".to_string(), "api".to_string())],
            points: vec![(0, 1.0), (1_000, 2.5)],
        };
        let res = query_response(QueryResult::Matrix(vec![series]), None, 0, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[0,"1"],[1,"2.5"]]}]}}"#
        );
    }

    /// `single_histogram` (`native_histograms.test:34`, A3/A5b corpus
    /// fixture): schema 0, `sum:5 count:4`, positive buckets (absolute)
    /// `[1,2,1]` at schema-index 0 -> (0.5,1]:1, (1,2]:2, (2,4]:1.
    fn single_float_histogram() -> pulsus_model::FloatHistogram {
        pulsus_model::FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 5.0,
            positive_spans: vec![pulsus_model::Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1.0, 2.0, 1.0],
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    // -- M7-A5b-i: the native-histogram wire encoder (MarshalHistogram) --

    #[tokio::test]
    async fn hist_vector_envelope_encodes_a_native_histogram_value() {
        let sample = HistVectorSample {
            labels: vec![("job".to_string(), "api".to_string())],
            value: HistOrFloat::Hist(Box::new(single_float_histogram())),
        };
        let res = query_response(QueryResult::VectorHist(vec![sample]), None, 5_000, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"histogram":[5,{"count":"4","sum":"5","buckets":[[0,"0.5","1","1"],[0,"1","2","2"],[0,"2","4","1"]]}]}]}}"#
        );
    }

    #[tokio::test]
    async fn hist_vector_envelope_float_element_still_uses_the_value_key() {
        let sample = HistVectorSample {
            labels: vec![("job".to_string(), "api".to_string())],
            value: HistOrFloat::Float(7.0),
        };
        let res = query_response(QueryResult::VectorHist(vec![sample]), None, 1_000, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[1,"7"]}]}}"#
        );
    }

    #[tokio::test]
    async fn hist_matrix_envelope_splits_values_and_histograms_into_separate_arrays() {
        let series = HistMatrixSeries {
            labels: vec![("job".to_string(), "api".to_string())],
            points: vec![
                (0, HistOrFloat::Float(1.0)),
                (1_000, HistOrFloat::Hist(Box::new(single_float_histogram()))),
            ],
        };
        let res = query_response(QueryResult::MatrixHist(vec![series]), None, 0, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[0,"1"]],"histograms":[[1,{"count":"4","sum":"5","buckets":[[0,"0.5","1","1"],[0,"1","2","2"],[0,"2","4","1"]]}]]}]}}"#
        );
    }

    #[tokio::test]
    async fn hist_matrix_envelope_omits_the_values_key_when_every_point_is_a_histogram() {
        let series = HistMatrixSeries {
            labels: vec![("job".to_string(), "api".to_string())],
            points: vec![(0, HistOrFloat::Hist(Box::new(single_float_histogram())))],
        };
        let res = query_response(QueryResult::MatrixHist(vec![series]), None, 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(json["data"]["result"][0].get("values").is_none());
        assert!(json["data"]["result"][0]["histograms"].is_array());
    }

    // -- M7-A5b-i: the `warnings`/`infos` envelope (top-level, omitempty) --

    #[tokio::test]
    async fn annotations_envelope_is_empty_for_a_float_only_query_byte_identical() {
        // query_response (the pre-A5b-i signature) and
        // query_response_annotated with an empty Annotations must render
        // byte-identically — AC6's "float responses stay byte-identical".
        let sample = VectorSample {
            labels: vec![("job".to_string(), "api".to_string())],
            value: 42.0,
        };
        let plain = query_response(
            QueryResult::Vector(vec![sample.clone()]),
            None,
            5_500,
            false,
        );
        let annotated = query_response_annotated(
            QueryResult::Vector(vec![sample]),
            None,
            5_500,
            "up",
            pulsus_promql::Annotations::new(),
            ResponseShape {
                ordered: false,
                limit: None,
                stats_requested: false,
            },
        );
        assert_eq!(body_string(plain).await, body_string(annotated).await);
    }

    #[tokio::test]
    async fn annotations_envelope_adds_warnings_and_infos_as_top_level_siblings_of_data() {
        let mut annos = pulsus_promql::Annotations::new();
        annos.warning_at(0, "w1");
        annos.info_at(3, "i1");
        // Issue #128: the envelope strings carry the byte-exact
        // `" (<line>:<col>)"` source-position suffix rendered against the
        // SAME raw query string the handler parsed.
        let res =
            query_response_annotated(QueryResult::Scalar(1.0), None, 0, "a + b", annos, NO_SHAPE);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"scalar","result":[0,"1"]},"warnings":["w1 (1:1)"],"infos":["i1 (1:4)"]}"#
        );
    }

    /// Issue #128 (AC 6): the byte-exact instant-query envelope for a
    /// REAL annotation over a multi-line query — line/column derive from
    /// byte offsets across the `\n`.
    #[tokio::test]
    async fn annotations_envelope_renders_the_position_suffix_for_a_multi_line_query() {
        let query = "histogram_quantile(\n  1.5,\n  up\n)";
        let mut annos = pulsus_promql::Annotations::new();
        // args[0] (`1.5`) at byte 22 — one '\n' at 19 → line 2, col 3.
        annos.warning_at(
            22,
            "PromQL warning: quantile value should be between 0 and 1, got 1.5",
        );
        let res =
            query_response_annotated(QueryResult::Scalar(1.5), None, 0, query, annos, NO_SHAPE);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"scalar","result":[0,"1.5"]},"warnings":["PromQL warning: quantile value should be between 0 and 1, got 1.5 (2:3)"]}"#
        );
    }

    /// Issue #128 (AC 6): the range (matrix) response envelope carries
    /// the same suffixed strings.
    #[tokio::test]
    async fn annotations_envelope_renders_the_position_suffix_for_a_range_response() {
        let query = "sort(up)";
        let mut annos = pulsus_promql::Annotations::new();
        annos.warning_at(
            0,
            "PromQL warning: sort is ineffective for range queries since results are always ordered by labels",
        );
        let series = MatrixSeries {
            labels: vec![("job".to_string(), "api".to_string())],
            points: vec![(0, 1.0)],
        };
        let res = query_response_annotated(
            QueryResult::Matrix(vec![series]),
            None,
            0,
            query,
            annos,
            NO_SHAPE,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[0,"1"]]}]},"warnings":["PromQL warning: sort is ineffective for range queries since results are always ordered by labels (1:1)"]}"#
        );
    }

    #[tokio::test]
    async fn annotations_envelope_dedups_caps_at_10_and_appends_the_overflow_line() {
        let mut annos = pulsus_promql::Annotations::new();
        for i in 0..12 {
            annos.warning_at(0, format!("w{i}"));
        }
        annos.warning_at(0, "w0"); // duplicate -- deduped, not double-counted.
        // Issue #128: the kept lines carry suffixes; the overflow line
        // (asserted below) never does.
        let res =
            query_response_annotated(QueryResult::Scalar(1.0), None, 0, "up", annos, NO_SHAPE);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        let warnings = json["warnings"].as_array().expect("warnings array");
        assert_eq!(warnings.len(), 11, "10 kept + 1 overflow line");
        assert_eq!(warnings[10], "2 more warning annotations omitted");
        assert!(json.get("infos").is_none(), "omitempty: no infos at all");
    }

    // -- issue #499: `limit` and `stats` on the query routes -----------

    fn shape(ordered: bool, limit: Option<usize>) -> ResponseShape {
        ResponseShape {
            ordered,
            limit,
            stats_requested: false,
        }
    }

    fn lbl(v: &str) -> Vec<(String, String)> {
        vec![("i".to_string(), v.to_string())]
    }

    /// The three results E1 truncates, labelled `c`, `a`, `b` in that order.
    fn results() -> [(&'static str, QueryResult); 4] {
        let vector = || {
            QueryResult::Vector(
                ["c", "a", "b"]
                    .map(|v| VectorSample {
                        labels: lbl(v),
                        value: 1.0,
                    })
                    .into(),
            )
        };
        let vector_hist = QueryResult::VectorHist(
            ["c", "a", "b"]
                .map(|v| HistVectorSample {
                    labels: lbl(v),
                    value: HistOrFloat::Float(1.0),
                })
                .into(),
        );
        let matrix = QueryResult::Matrix(
            ["c", "a", "b"]
                .map(|v| MatrixSeries {
                    labels: lbl(v),
                    points: vec![(0, 1.0)],
                })
                .into(),
        );
        let matrix_hist = QueryResult::MatrixHist(
            ["c", "a", "b"]
                .map(|v| HistMatrixSeries {
                    labels: lbl(v),
                    points: vec![(0, HistOrFloat::Float(1.0))],
                })
                .into(),
        );
        [
            ("vector", vector()),
            ("vector", vector_hist),
            ("matrix", matrix),
            ("matrix", matrix_hist),
        ]
    }

    /// The labels of a body's result, in order, and its `warnings`.
    async fn kept(res: Response) -> (Vec<String>, Option<serde_json::Value>, String) {
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).expect("json");
        let labels = json["data"]["result"]
            .as_array()
            .expect("result array")
            .iter()
            .map(|r| r["metric"]["i"].as_str().expect("label").to_string())
            .collect();
        (labels, json.get("warnings").cloned(), body)
    }

    /// E1: a vector is cut in the evaluator's order and then sorted; a
    /// matrix is sorted and then cut; the warning appears only when the
    /// limit cut something, and never on a scalar.
    #[tokio::test]
    async fn limit_truncates_as_the_reference_does() {
        let warning = serde_json::json!(["results truncated due to limit"]);
        for (i, (kind, result)) in results().into_iter().enumerate() {
            let want: &[&str] = if kind == "vector" {
                &["a", "c"]
            } else {
                &["a", "b"]
            };
            let res = query_response_annotated(
                result,
                None,
                0,
                "up",
                pulsus_promql::Annotations::new(),
                shape(false, Some(2)),
            );
            let (labels, warnings, body) = kept(res).await;
            assert_eq!(labels, want, "result {i}: {body}");
            assert_eq!(warnings, Some(warning.clone()), "result {i}: {body}");
            assert!(
                body.ends_with(r#"]},"warnings":["results truncated due to limit"]}"#),
                "result {i}: {body}"
            );
        }
        for (i, (kind, result)) in results().into_iter().enumerate() {
            if kind != "vector" {
                continue;
            }
            let res = query_response_annotated(
                result,
                None,
                0,
                "up",
                pulsus_promql::Annotations::new(),
                shape(true, Some(2)),
            );
            let (labels, _, body) = kept(res).await;
            assert_eq!(labels, ["c", "a"], "result {i} ordered: {body}");
        }
        for (i, (_, result)) in results().into_iter().enumerate() {
            let res = query_response_annotated(
                result,
                None,
                0,
                "up",
                pulsus_promql::Annotations::new(),
                shape(false, Some(3)),
            );
            let (labels, warnings, body) = kept(res).await;
            assert_eq!(labels.len(), 3, "result {i}: {body}");
            assert_eq!(warnings, None, "result {i}: {body}");
        }
        let res = query_response_annotated(
            QueryResult::Scalar(1.0),
            None,
            0,
            "1",
            pulsus_promql::Annotations::new(),
            shape(false, Some(1)),
        );
        let body = body_string(res).await;
        assert!(!body.contains("warnings"), "{body}");
    }

    /// E2: the truncation warning follows the evaluation's own warnings.
    #[tokio::test]
    async fn the_truncation_warning_follows_the_evaluations_warnings() {
        let mut annos = pulsus_promql::Annotations::new();
        annos.warning_at(0, "w");
        let result = QueryResult::Vector(
            ["a", "b"]
                .map(|v| VectorSample {
                    labels: lbl(v),
                    value: 1.0,
                })
                .into(),
        );
        let res = query_response_annotated(result, None, 0, "up", annos, shape(false, Some(1)));
        let (_, warnings, body) = kept(res).await;
        assert_eq!(
            warnings,
            Some(serde_json::json!([
                "w (1:1)",
                "results truncated due to limit"
            ])),
            "{body}"
        );
    }

    /// E3: the `stats` notice comes after the 10-warning cap and its
    /// overflow line, so the cap never drops it.
    #[tokio::test]
    async fn the_stats_notice_is_last_and_outside_the_cap() {
        let stats = ResponseShape {
            ordered: false,
            limit: None,
            stats_requested: true,
        };
        let mut annos = pulsus_promql::Annotations::new();
        for i in 0..11 {
            annos.warning_at(0, format!("w{i}"));
        }
        let res = query_response_annotated(QueryResult::Scalar(1.0), None, 0, "up", annos, stats);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).expect("json");
        let mut want: Vec<String> = (0..10).map(|i| format!("w{i} (1:1)")).collect();
        want.push("1 more warning annotations omitted".to_string());
        want.push(STATS_UNSUPPORTED_WARNING.to_string());
        assert_eq!(json["warnings"], serde_json::json!(want), "{body}");

        let res = query_response_annotated(
            QueryResult::Scalar(1.0),
            None,
            0,
            "up",
            pulsus_promql::Annotations::new(),
            stats,
        );
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(
            json["warnings"],
            serde_json::json!([STATS_UNSUPPORTED_WARNING]),
            "{body}"
        );
    }

    #[tokio::test]
    async fn scalar_envelope_is_byte_exact() {
        let res = query_response(QueryResult::Scalar(2.0), None, 1_000, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}"#
        );
    }

    /// Issue #86 (M6-08d): a top-level string-literal query renders the
    /// Prometheus `resultType:"string"` shape, timestamp stamped from the
    /// request's evaluation time and the value JSON-escaped.
    #[tokio::test]
    async fn string_envelope_is_byte_exact() {
        let res = query_response(QueryResult::String("Foo".to_string()), None, 1_000, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"string","result":[1,"Foo"]}}"#
        );
        // Escaping + fractional eval time.
        let res = query_response(
            QueryResult::String("a\"b\\c".to_string()),
            None,
            1_500,
            false,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"string","result":[1.5,"a\"b\\c"]}}"#
        );
    }

    /// Code-review round-1 fix: `explain` on a scalar result must nest
    /// under `data` exactly like vector/matrix — never a top-level
    /// sibling of `data`.
    #[tokio::test]
    async fn scalar_envelope_nests_explain_under_data_byte_exact() {
        let mut explain = PlanExplain::new("scalar");
        explain.push("literal", "SELECT 2", None);
        let res = query_response(QueryResult::Scalar(2.0), Some(explain), 1_000, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"scalar","result":[1,"2"],"explain":{"result_type":"scalar","exactness":"raw-exact","stages":[{"name":"literal","sql":"SELECT 2","note":null}]}}}"#
        );
    }

    #[tokio::test]
    async fn scalar_envelope_with_explain_has_no_top_level_explain_field() {
        let mut explain = PlanExplain::new("scalar");
        explain.push("literal", "SELECT 2", None);
        let res = query_response(QueryResult::Scalar(2.0), Some(explain), 1_000, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(
            json.get("explain").is_none(),
            "explain must not be a top-level sibling of data: {json}"
        );
        assert_eq!(json["data"]["explain"]["result_type"], "scalar");
    }

    #[tokio::test]
    async fn vector_envelope_sorts_multiple_series_by_label_set() {
        let result = QueryResult::Vector(vec![
            VectorSample {
                labels: vec![("job".to_string(), "zeta".to_string())],
                value: 1.0,
            },
            VectorSample {
                labels: vec![("job".to_string(), "alpha".to_string())],
                value: 2.0,
            },
        ]);
        let res = query_response(result, None, 0, false);
        let body = body_string(res).await;
        let alpha_pos = body.find("alpha").expect("alpha present");
        let zeta_pos = body.find("zeta").expect("zeta present");
        assert!(alpha_pos < zeta_pos);
    }

    /// Issue #68 (M6-05) gate, direction 1: `ordered: true` (a
    /// sort-rooted instant query) preserves the evaluator's value order
    /// on the wire — the label re-sort must NOT run (it would put
    /// `alpha` first and make `sort()` inert at the API).
    #[tokio::test]
    async fn ordered_vector_envelope_preserves_evaluator_order_on_the_wire() {
        let result = QueryResult::Vector(vec![
            VectorSample {
                labels: vec![("job".to_string(), "zeta".to_string())],
                value: 1.0,
            },
            VectorSample {
                labels: vec![("job".to_string(), "alpha".to_string())],
                value: 2.0,
            },
        ]);
        let res = query_response(result, None, 0, true);
        let body = body_string(res).await;
        let alpha_pos = body.find("alpha").expect("alpha present");
        let zeta_pos = body.find("zeta").expect("zeta present");
        assert!(
            zeta_pos < alpha_pos,
            "ordered=true must keep the evaluator's order (zeta first): {body}"
        );
    }

    /// Issue #68 (M6-05) gate, direction 2: `ordered` never disturbs a
    /// matrix result — range queries keep the deterministic label sort
    /// regardless (upstream's own "sort is ineffective for range
    /// queries").
    #[tokio::test]
    async fn ordered_flag_does_not_affect_matrix_label_sorting() {
        let make = || {
            QueryResult::Matrix(vec![
                MatrixSeries {
                    labels: vec![("job".to_string(), "zeta".to_string())],
                    points: vec![(0, 1.0)],
                },
                MatrixSeries {
                    labels: vec![("job".to_string(), "alpha".to_string())],
                    points: vec![(0, 2.0)],
                },
            ])
        };
        for ordered in [false, true] {
            let res = query_response(make(), None, 0, ordered);
            let body = body_string(res).await;
            let alpha_pos = body.find("alpha").expect("alpha present");
            let zeta_pos = body.find("zeta").expect("zeta present");
            assert!(
                alpha_pos < zeta_pos,
                "matrix stays label-sorted with ordered={ordered}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn query_response_carries_explain_with_exactness() {
        let mut explain = PlanExplain::new("vector");
        explain.push("sample_fetch", "SELECT 1", None);
        let res = query_response(QueryResult::Vector(vec![]), Some(explain), 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["explain"]["result_type"], "vector");
        assert_eq!(json["data"]["explain"]["exactness"], "raw-exact");
        assert_eq!(json["data"]["explain"]["stages"][0]["name"], "sample_fetch");
    }

    /// **B9 (issue #18 part B): `exactness` follows what served.** A
    /// trace with `rollup` false is `"raw-exact"`, one with `rollup` true
    /// `"rollup"`.
    #[test]
    fn b9_exactness_is_rollup_when_a_level_served() {
        let mut explain = PlanExplain::new("matrix");
        explain.push("sample_fetch", "SELECT 1", None);
        assert!(!explain.rollup, "a new trace is raw-exact");
        assert!(
            explain_json(&explain).contains(r#""exactness":"raw-exact""#),
            "{}",
            explain_json(&explain)
        );
        explain.rollup = true;
        assert!(
            explain_json(&explain).contains(r#""exactness":"rollup""#),
            "{}",
            explain_json(&explain)
        );
    }

    /// Issue #548 criterion 5, half one: **the explain surface gains
    /// `plans` and loses nothing.**
    ///
    /// Two responses, both byte-exact:
    ///
    /// 1. no header at all — no `explain` key;
    /// 2. the header, on a query whose plan produced no chain that reads
    ///    (`time()` sends no statement) — an `explain` object with the
    ///    same three keys it carried before `plans` existed.
    ///
    /// The second is what the `skip_serializing_if` on `plans` buys, and
    /// it is why this test and not another one reddens when that
    /// attribute is dropped.
    #[tokio::test]
    async fn an_unexplained_metrics_response_is_byte_identical() {
        let res = query_response(QueryResult::Scalar(2.0), None, 1_000, false);
        assert_eq!(
            body_string(res).await,
            r#"{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}"#,
            "a response without the header carries no explain key at all"
        );

        let mut explain = PlanExplain::new("scalar");
        explain.push("literal", "SELECT 2", None);
        assert!(explain.plans.is_empty(), "time() reads no selector");
        let res = query_response(QueryResult::Scalar(2.0), Some(explain), 1_000, false);
        assert_eq!(
            body_string(res).await,
            r#"{"status":"success","data":{"resultType":"scalar","result":[1,"2"],"explain":{"result_type":"scalar","exactness":"raw-exact","stages":[{"name":"literal","sql":"SELECT 2","note":null}]}}}"#,
            "an explain object with no plan is byte-identical to one from before the key existed"
        );
    }

    /// Issue #548 criterion 5, half two: with plans present the three
    /// existing keys keep their names, their values and their order, and
    /// `plans` is the fourth.
    #[tokio::test]
    async fn the_explain_object_keeps_result_type_exactness_and_stages() {
        let mut explain = PlanExplain::new("matrix");
        explain.push("series_resolution", "SELECT fingerprint", None);
        explain.push("sample_fetch", "SELECT value", Some("note".to_string()));
        explain.push_plan(pulsus_read::compile::plan::PlanShape {
            parts: vec![pulsus_read::compile::plan::PartShape::Engine(
                pulsus_read::compile::plan::EnginePartShape {
                    kind: "engine",
                    links: vec![0],
                },
            )],
            links: vec![pulsus_read::compile::plan::LinkShape {
                i: 0,
                part: 0,
                stage: "Select(0)".to_string(),
                how: "lowered",
                fidelity: Some("wider"),
                why: None,
            }],
        });
        let res = query_response(QueryResult::Matrix(Vec::new()), Some(explain), 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).expect("json");
        let e = &json["data"]["explain"];
        assert_eq!(e["result_type"], "matrix");
        assert_eq!(e["exactness"], "raw-exact");
        assert_eq!(e["stages"][0]["name"], "series_resolution");
        assert_eq!(e["stages"][0]["sql"], "SELECT fingerprint");
        assert_eq!(e["stages"][0]["note"], serde_json::Value::Null);
        assert_eq!(e["stages"][1]["name"], "sample_fetch");
        assert_eq!(e["stages"][1]["note"], "note");
        assert_eq!(e["plans"][0]["links"][0]["stage"], "Select(0)");
        assert_eq!(e["plans"][0]["parts"][0]["kind"], "engine");
        // The key ORDER, which a `Value` comparison cannot see: the
        // three existing keys open the object exactly as they did, and
        // `plans` follows them.
        let object = body.split_once(r#""explain":"#).expect("explain object").1;
        assert!(
            object.starts_with(
                r#"{"result_type":"matrix","exactness":"raw-exact","stages":[{"name":"series_resolution""#
            ),
            "the three existing keys keep their order: {object}"
        );
        let stages_at = object.find(r#""stages":"#).expect("stages key");
        let plans_at = object.find(r#""plans":"#).expect("plans key");
        assert!(
            stages_at < plans_at,
            "`plans` is the fourth key, after `stages`: {object}"
        );
    }

    #[tokio::test]
    async fn empty_vector_result_still_renders_a_well_formed_envelope() {
        let res = query_response(QueryResult::Vector(vec![]), None, 0, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#
        );
    }

    // --- string_array_response / series_response ---

    #[tokio::test]
    async fn string_array_envelope_is_byte_exact() {
        let res = string_array_response(vec!["__name__".to_string(), "job".to_string()], None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":["__name__","job"]}"#);
    }

    #[tokio::test]
    async fn empty_string_array_response_renders_an_empty_data_array() {
        let res = string_array_response(vec![], None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":[]}"#);
    }

    #[tokio::test]
    async fn series_envelope_is_byte_exact_for_a_single_series() {
        let res = series_response(
            vec![vec![
                ("__name__".to_string(), "up".to_string()),
                ("job".to_string(), "api".to_string()),
            ]],
            None,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"__name__":"up","job":"api"}]}"#
        );
    }

    /// Issue #471 M4, criterion 19: the truncation warning is a TOP-LEVEL
    /// sibling of `data`, never nested inside it, and `None` keeps today's
    /// bytes exactly.
    #[tokio::test]
    async fn string_array_response_appends_the_warning_as_a_top_level_sibling() {
        let res = string_array_response(vec!["a".to_string()], Some(TRUNCATION_WARNING));
        assert_eq!(
            body_string(res).await,
            r#"{"status":"success","data":["a"],"warnings":["results truncated due to limit"]}"#
        );
        let res = string_array_response(vec!["a".to_string()], None);
        assert_eq!(
            body_string(res).await,
            r#"{"status":"success","data":["a"]}"#
        );
    }

    #[tokio::test]
    async fn series_response_appends_the_warning_as_a_top_level_sibling() {
        let res = series_response(
            vec![vec![("__name__".to_string(), "up".to_string())]],
            Some(TRUNCATION_WARNING),
        );
        assert_eq!(
            body_string(res).await,
            concat!(
                r#"{"status":"success","data":[{"__name__":"up"}],"#,
                r#""warnings":["results truncated due to limit"]}"#
            )
        );
    }

    /// An empty `data` array still carries the warning outside it — the
    /// suffix is the only place the bytes can come from, so a `limit` that
    /// truncated everything cannot silently lose the warning.
    #[tokio::test]
    async fn an_empty_array_still_carries_the_warning_outside_data() {
        let res = string_array_response(vec![], Some(TRUNCATION_WARNING));
        assert_eq!(
            body_string(res).await,
            r#"{"status":"success","data":[],"warnings":["results truncated due to limit"]}"#
        );
    }

    // --- metadata_response ---

    #[tokio::test]
    async fn metadata_envelope_is_byte_exact() {
        let res = metadata_response(vec![MetricMeta {
            name: "up".to_string(),
            metric_type: "gauge".to_string(),
            help: "1 if healthy".to_string(),
            unit: "".to_string(),
        }]);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"up":[{"type":"gauge","help":"1 if healthy","unit":""}]}}"#
        );
    }

    #[tokio::test]
    async fn metadata_envelope_sorts_by_metric_name() {
        let res = metadata_response(vec![
            MetricMeta {
                name: "zeta".to_string(),
                metric_type: "counter".to_string(),
                help: "".to_string(),
                unit: "".to_string(),
            },
            MetricMeta {
                name: "alpha".to_string(),
                metric_type: "counter".to_string(),
                help: "".to_string(),
                unit: "".to_string(),
            },
        ]);
        let body = body_string(res).await;
        let alpha_pos = body.find("alpha").expect("alpha present");
        let zeta_pos = body.find("zeta").expect("zeta present");
        assert!(alpha_pos < zeta_pos);
    }

    /// Issue #500, T4: one key per name, its array holding every entry of
    /// that name in row order.
    #[tokio::test]
    async fn one_key_per_name_with_every_entry() {
        let meta = |name: &str, t: &str, help: &str, unit: &str| MetricMeta {
            name: name.to_string(),
            metric_type: t.to_string(),
            help: help.to_string(),
            unit: unit.to_string(),
        };
        let res = metadata_response(vec![
            meta("m", "counter", "A", ""),
            meta("m", "gauge", "A", ""),
            meta("m", "gauge", "A", "seconds"),
            meta("n", "gauge", "N", ""),
        ]);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"m":[{"type":"counter","help":"A","unit":""},{"type":"gauge","help":"A","unit":""},{"type":"gauge","help":"A","unit":"seconds"}],"n":[{"type":"gauge","help":"N","unit":""}]}}"#
        );
    }

    // --- query_exemplars (issue #501 part 2) ---

    /// U5: no selector is the bare success; an answer is the reference's
    /// shape byte for byte.
    #[tokio::test]
    async fn query_exemplars_response_renders_both_shapes() {
        assert_eq!(
            body_string(query_exemplars_response(None)).await,
            r#"{"status":"success"}"#
        );
        assert_eq!(
            body_string(query_exemplars_response(Some(Vec::new()))).await,
            r#"{"status":"success","data":[]}"#
        );
        let answer = vec![ExemplarSeries {
            series_labels: vec![("__name__".to_string(), "m".to_string())],
            exemplars: vec![StoredExemplar {
                labels: vec![(
                    "trace_id".to_string(),
                    "0102030405060708090a0b0c0d0e0f10".to_string(),
                )],
                value: 0.25,
                unix_milli: 1_500,
            }],
        }];
        assert_eq!(
            body_string(query_exemplars_response(Some(answer))).await,
            r#"{"status":"success","data":[{"seriesLabels":{"__name__":"m"},"exemplars":[{"labels":{"trace_id":"0102030405060708090a0b0c0d0e0f10"},"value":"0.25","timestamp":1.5}]}]}"#
        );
    }

    /// U3f (issue #501 part 2, r8): per row, the bytes its object and
    /// separator add to the body are at most its `_json` half — an
    /// exemplar with labels `{}`, both ids, the longest value and the
    /// widest timestamp, which needs the fixed 464; the other extreme
    /// values; labels of 1,000 control characters; a series named with 100
    /// control characters, which needs the factor 6 — and the whole body is
    /// at most the sum of the `_json` halves and the envelope's 64.
    #[tokio::test]
    async fn each_rows_json_is_never_longer_than_its_json_charge() {
        use pulsus_model::Fingerprint;
        use pulsus_read::metrics::exemplars::{
            ENVELOPE_CHARGE, assemble, exemplar_row_json, keep_exemplar, keep_series,
            series_row_json,
        };
        use pulsus_read::metrics::rows::{ExemplarRow, ExemplarSeriesRow};
        let controls = |n: u32| -> String {
            (0..n)
                .map(|i| char::from_u32(1 + i % 31).expect("a control character"))
                .collect()
        };
        let wide_labels = serde_json::to_string(&serde_json::json!({ "k": controls(1_000) }))
            .expect("canonical labels");
        // The largest subnormal, 2.2250738585072009e-308.
        let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
        let row = |value: f64, labels: &str| ExemplarRow {
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 4_294_967_295_999,
            value,
            trace_id: [0xab; 16],
            span_id: [0xcd; 8],
            labels: labels.to_string(),
        };
        let rows = vec![
            row(-5e-324, "{}"),
            row(largest_subnormal, "{}"),
            row(f64::MAX, "{}"),
            row(f64::NAN, "{}"),
            row(-5e-324, &wide_labels),
        ];
        let series = vec![
            ExemplarSeriesRow {
                fingerprint: Fingerprint::from_raw(1),
                series_name: controls(100),
                series_labels: "{}".to_string(),
            },
            ExemplarSeriesRow {
                fingerprint: Fingerprint::from_raw(2),
                series_name: "m".to_string(),
                series_labels: wide_labels.clone(),
            },
        ];
        let body_len = |answer: Vec<ExemplarSeries>| async move {
            body_string(query_exemplars_response(Some(answer)))
                .await
                .len()
        };
        let host = || ExemplarSeries {
            series_labels: vec![("__name__".to_string(), "m".to_string())],
            exemplars: Vec::new(),
        };
        let bare = body_len(vec![host()]).await;
        for r in &rows {
            let mut one = host();
            one.exemplars.push(keep_exemplar(r.clone()).1);
            let bytes = body_len(vec![one]).await - bare + 1;
            assert!(
                bytes <= exemplar_row_json(r),
                "an exemplar takes {bytes} bytes against its json charge of {}: {r:?}",
                exemplar_row_json(r)
            );
        }
        let empty = body_len(Vec::new()).await;
        for s in &series {
            let (_, series_labels) = keep_series(s.clone());
            let bytes = body_len(vec![ExemplarSeries {
                series_labels,
                exemplars: Vec::new(),
            }])
            .await
                - empty
                + 1;
            assert!(
                bytes <= series_row_json(s),
                "a series takes {bytes} bytes against its json charge of {}",
                series_row_json(s)
            );
        }
        let charged = ENVELOPE_CHARGE
            + rows.iter().map(exemplar_row_json).sum::<usize>()
            + series.iter().map(series_row_json).sum::<usize>();
        let n = rows.len();
        let answer = assemble(
            rows.into_iter().map(keep_exemplar).collect(),
            series.into_iter().map(keep_series).collect(),
        );
        let body = body_string(query_exemplars_response(Some(answer))).await;
        let json: serde_json::Value = serde_json::from_str(&body).expect("the answer is JSON");
        let kept: usize = json["data"]
            .as_array()
            .expect("data")
            .iter()
            .map(|s| s["exemplars"].as_array().expect("exemplars").len())
            .sum();
        assert_eq!(kept, n, "every row is in the answer");
        assert!(
            body.len() <= charged,
            "the body is {} bytes against a json charge of {charged}",
            body.len()
        );
        assert_eq!(quoted_value(-5e-324).len(), 329);
    }

    // --- status/* ---

    #[tokio::test]
    async fn status_buildinfo_maps_fields() {
        let build = BuildInfo {
            version: "1.2.3".to_string(),
            revision: "abc123".to_string(),
            built_at: "2026-01-01T00:00:00Z".to_string(),
            rustc: "1.80.0".to_string(),
        };
        let res = status_buildinfo_response(&build);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["version"], "1.2.3");
        assert_eq!(json["data"]["revision"], "abc123");
        assert_eq!(json["data"]["branch"], "");
        assert_eq!(json["data"]["buildUser"], "");
        assert_eq!(json["data"]["buildDate"], "2026-01-01T00:00:00Z");
        assert_eq!(json["data"]["goVersion"], "1.80.0");
    }

    #[tokio::test]
    async fn status_config_wraps_the_yaml_string() {
        let res = status_config_response("mode: all\n");
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["yaml"], "mode: all\n");
    }

    #[tokio::test]
    async fn status_flags_is_an_empty_object() {
        let res = status_flags_response();
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":{}}"#);
    }

    #[tokio::test]
    async fn status_runtimeinfo_maps_start_time_and_retention() {
        let res =
            status_runtimeinfo_response("2026-01-01T00:00:00+00:00".to_string(), 7, Vec::new());
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["startTime"], "2026-01-01T00:00:00+00:00");
        assert_eq!(json["data"]["storageRetention"], "7d");
    }

    #[tokio::test]
    async fn status_tsdb_maps_head_stats_and_cardinality() {
        let status = TsdbStatus {
            num_series: 42,
            series_count_by_metric_name: vec![("up".to_string(), 10)],
        };
        let res = status_tsdb_response(status);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["headStats"]["numSeries"], 42);
        assert!(
            json["data"]["headStats"].get("numSamples").is_none(),
            "numSamples must be omitted (code-review round-1 fix): {json}"
        );
        assert_eq!(json["data"]["seriesCountByMetricName"][0]["name"], "up");
        assert_eq!(json["data"]["seriesCountByMetricName"][0]["value"], 10);
    }

    // --- streaming/poll-after-end/gzip (issue #24 regression, mirrors
    // logs_api::encode's own coverage) ---

    #[tokio::test]
    async fn stream_array_body_yields_none_instead_of_panicking_when_polled_after_completion() {
        let result = QueryResult::Vector(vec![VectorSample {
            labels: vec![("job".to_string(), "api".to_string())],
            value: 1.0,
        }]);
        let res = query_response(result, None, 0, false);
        let mut body_stream = res.into_body().into_data_stream();

        while body_stream.next().await.is_some() {}

        assert!(
            body_stream.next().await.is_none(),
            "polling the body stream once more after completion must yield None, not panic"
        );
    }

    /// Encoder memory bound (issue #24 / architect plan AC): drives a
    /// synthetic large matrix result through the raw chunk stream and
    /// asserts every individual yielded chunk stays near one series' own
    /// size — never anywhere close to the full aggregate.
    #[tokio::test]
    async fn matrix_encoder_yields_bounded_chunks_for_a_large_synthetic_result() {
        const NUM_SERIES: usize = 1_000;
        const POINTS_PER_SERIES: usize = 200;

        let items: Vec<MatrixSeries> = (0..NUM_SERIES)
            .map(|i| MatrixSeries {
                labels: vec![("series".to_string(), format!("s-{i:05}"))],
                points: (0..POINTS_PER_SERIES)
                    .map(|j| ((i * POINTS_PER_SERIES + j) as i64 * 1000, j as f64))
                    .collect(),
            })
            .collect();

        let res = query_response(QueryResult::Matrix(items), None, 0, false);
        let mut stream = res.into_body().into_data_stream();

        let mut chunk_count = 0usize;
        let mut max_chunk_len = 0usize;
        let mut total_len = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("chunk");
            chunk_count += 1;
            max_chunk_len = max_chunk_len.max(chunk.len());
            total_len += chunk.len();
        }

        assert_eq!(chunk_count, NUM_SERIES + 2);
        assert!(total_len > 1_000_000, "total_len = {total_len}");
        assert!(
            max_chunk_len < 64 * 1024,
            "max_chunk_len = {max_chunk_len} (aggregate would be ~{total_len})"
        );
    }

    async fn assert_gzip_response_is_byte_identical_to_identity(build: fn() -> Response) {
        let router = Router::new()
            .route("/x", get(move || async move { build() }))
            .layer(crate::middleware::compression_layer());

        let identity_request = Request::builder().uri("/x").body(Body::empty()).unwrap();
        let identity_response = router
            .clone()
            .oneshot(identity_request)
            .await
            .expect("identity request must not panic the request task");
        let identity_body = to_bytes(identity_response.into_body(), usize::MAX)
            .await
            .expect("identity body");

        let gzip_request = Request::builder()
            .uri("/x")
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap();
        let gzip_response = router
            .oneshot(gzip_request)
            .await
            .expect("gzip request must not panic the request task (issue #24 regression)");
        assert_eq!(
            gzip_response
                .headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("gzip"),
            "response must actually be gzip-encoded for this assertion to be meaningful"
        );
        let gzip_body = to_bytes(gzip_response.into_body(), usize::MAX)
            .await
            .expect("gzip body");

        let mut decoder = GzDecoder::new(&gzip_body[..]);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .expect("gzip body must decode as a valid gzip stream");

        assert_eq!(
            decoded, identity_body,
            "gzip-decoded body must be byte-identical to the identity-encoding body"
        );
    }

    #[tokio::test]
    async fn gzip_vector_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let sample = VectorSample {
                labels: vec![("job".to_string(), "api".to_string())],
                value: 42.0,
            };
            query_response(QueryResult::Vector(vec![sample]), None, 5_500, false)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_series_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            series_response(vec![vec![("__name__".to_string(), "up".to_string())]], None)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    // -- issue #795: the writers append to one buffer, and write what
    //    they wrote before. The `_before` functions are the writers as they
    //    stood before the change, copied verbatim. --

    fn prom_float_before(v: f64) -> String {
        if v.is_nan() {
            return "NaN".to_string();
        }
        if v.is_infinite() {
            return if v.is_sign_positive() {
                "+Inf".to_string()
            } else {
                "-Inf".to_string()
            };
        }
        if v == 0.0 {
            return if v.is_sign_negative() {
                "-0".to_string()
            } else {
                "0".to_string()
            };
        }

        let sci = format!("{v:e}");
        let (mantissa_part, exp_str) = sci
            .split_once('e')
            .expect("Rust's {v:e} formatter always emits exactly one 'e'");
        let exp: i32 = exp_str
            .parse()
            .expect("Rust's {v:e} exponent is always a valid integer literal");

        let negative = mantissa_part.starts_with('-');
        let mantissa = mantissa_part.strip_prefix('-').unwrap_or(mantissa_part);
        let digits: String = mantissa.chars().filter(|c| *c != '.').collect();

        let body = render_fixed_before(&digits, exp);
        if negative { format!("-{body}") } else { body }
    }

    fn render_fixed_before(digits: &str, exp: i32) -> String {
        let point = exp + 1;
        if point <= 0 {
            let zeros = "0".repeat((-point) as usize);
            format!("0.{zeros}{digits}")
        } else if (point as usize) >= digits.len() {
            let zeros = "0".repeat(point as usize - digits.len());
            format!("{digits}{zeros}")
        } else {
            let (whole, frac) = digits.split_at(point as usize);
            format!("{whole}.{frac}")
        }
    }

    fn prom_timestamp_before(ms: i64) -> String {
        let secs = ms.div_euclid(1000);
        let millis = ms.rem_euclid(1000);
        if millis == 0 {
            return secs.to_string();
        }
        let frac = format!("{millis:03}");
        let trimmed = frac.trim_end_matches('0');
        format!("{secs}.{trimmed}")
    }

    fn quoted_value_before(v: f64) -> String {
        json_string(&prom_float_before(v))
    }

    fn labels_object_json_before(labels: &[(String, String)]) -> String {
        let map: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".to_string())
    }

    fn render_matrix_item_before(s: &MatrixSeries) -> Vec<u8> {
        let mut points = String::new();
        for (i, (t_ms, value)) in s.points.iter().enumerate() {
            if i > 0 {
                points.push(',');
            }
            points.push_str(&format!(
                "[{},{}]",
                prom_timestamp_before(*t_ms),
                quoted_value_before(*value)
            ));
        }
        format!(
            "{{\"metric\":{},\"values\":[{}]}}",
            labels_object_json_before(&s.labels),
            points
        )
        .into_bytes()
    }

    fn render_vector_item_before(s: &VectorSample, at_ms: i64) -> Vec<u8> {
        format!(
            "{{\"metric\":{},\"value\":[{},{}]}}",
            labels_object_json_before(&s.labels),
            prom_timestamp_before(at_ms),
            quoted_value_before(s.value)
        )
        .into_bytes()
    }

    fn render_histogram_json_before(h: &pulsus_model::FloatHistogram) -> String {
        let mut buckets = String::new();
        let mut any = false;
        for b in h.all_buckets() {
            if b.count == 0.0 {
                continue;
            }
            if any {
                buckets.push(',');
            }
            any = true;
            let code = histogram_boundary_code(b.lower_inclusive, b.upper_inclusive);
            buckets.push_str(&format!(
                "[{code},{},{},{}]",
                quoted_value_before(b.lower),
                quoted_value_before(b.upper),
                quoted_value_before(b.count)
            ));
        }
        let buckets_field = if any {
            format!(",\"buckets\":[{buckets}]")
        } else {
            String::new()
        };
        format!(
            "{{\"count\":{},\"sum\":{}{buckets_field}}}",
            quoted_value_before(h.count),
            quoted_value_before(h.sum)
        )
    }

    fn render_hist_or_float_before(v: &HistOrFloat) -> String {
        match v {
            HistOrFloat::Float(f) => quoted_value_before(*f),
            HistOrFloat::Hist(h) => render_histogram_json_before(h),
        }
    }

    fn render_hist_vector_item_before(s: &HistVectorSample, at_ms: i64) -> Vec<u8> {
        let key = match &s.value {
            HistOrFloat::Float(_) => "value",
            HistOrFloat::Hist(_) => "histogram",
        };
        format!(
            "{{\"metric\":{},\"{key}\":[{},{}]}}",
            labels_object_json_before(&s.labels),
            prom_timestamp_before(at_ms),
            render_hist_or_float_before(&s.value)
        )
        .into_bytes()
    }

    fn render_hist_matrix_item_before(s: &HistMatrixSeries) -> Vec<u8> {
        let mut values = String::new();
        let mut any_values = false;
        let mut histograms = String::new();
        let mut any_histograms = false;
        for (t_ms, v) in &s.points {
            match v {
                HistOrFloat::Float(f) => {
                    if any_values {
                        values.push(',');
                    }
                    any_values = true;
                    values.push_str(&format!(
                        "[{},{}]",
                        prom_timestamp_before(*t_ms),
                        quoted_value_before(*f)
                    ));
                }
                HistOrFloat::Hist(h) => {
                    if any_histograms {
                        histograms.push(',');
                    }
                    any_histograms = true;
                    histograms.push_str(&format!(
                        "[{},{}]",
                        prom_timestamp_before(*t_ms),
                        render_histogram_json_before(h)
                    ));
                }
            }
        }
        let mut body = format!("{{\"metric\":{}", labels_object_json_before(&s.labels));
        if any_values {
            body.push_str(&format!(",\"values\":[{values}]"));
        }
        if any_histograms {
            body.push_str(&format!(",\"histograms\":[{histograms}]"));
        }
        body.push('}');
        body.into_bytes()
    }

    /// xorshift64*, seed `0x795`: the fixed stream the tests below draw from.
    struct XorShift(u64);

    impl XorShift {
        fn new() -> Self {
            XorShift(0x795)
        }

        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> usize {
            (self.next() % n) as usize
        }
    }

    /// Every value the existing `prom_float_*` tests name, and the specials.
    const PROM_FLOAT_NAMED: &[f64] = &[
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        1.0,
        -1.0,
        1.5,
        42.0,
        100_000.0,
        1e20,
        1e21,
        1e-4,
        1e-5,
        5e-324,
        f64::MAX,
        -1e-300,
        -100_000.0,
        0.016_666_666_666_666_666,
    ];

    /// E1's values: the named ones, every power of ten and its two
    /// neighbours, the integers to 100,000, and 2,000,000 random bit
    /// patterns.
    fn prom_float_inputs() -> Vec<f64> {
        let mut values: Vec<f64> = PROM_FLOAT_NAMED.to_vec();
        for k in -324..=308 {
            let p: f64 = format!("1e{k}").parse().expect("a power of ten");
            values.extend([p, p.next_up(), p.next_down()]);
        }
        values.extend((0..=100_000).map(f64::from));
        let mut rng = XorShift::new();
        values.extend((0..2_000_000).map(|_| f64::from_bits(rng.next())));
        values
    }

    /// **E1 (issue #795): `prom_float` writes what it wrote before**, and
    /// its text reads back to the same bits.
    #[test]
    fn prom_float_writes_what_it_wrote_before() {
        for v in prom_float_inputs() {
            let text = prom_float(v);
            assert_eq!(
                text,
                prom_float_before(v),
                "prom_float({v:e}), bits {:#x}",
                v.to_bits()
            );
            if v.is_finite() {
                assert!(!text.contains('e'), "no exponent in {text}");
                let back: f64 = text.parse().expect("the text is a number");
                assert_eq!(back.to_bits(), v.to_bits(), "{text} reads back");
            }
        }
    }

    /// **E2 (issue #795): `prom_timestamp` writes what it wrote before.**
    #[test]
    fn prom_timestamp_writes_what_it_wrote_before() {
        let mut values: Vec<i64> = (-3_000..=3_000).collect();
        values.extend([i64::MIN, i64::MAX, i64::MIN + 1]);
        let mut rng = XorShift::new();
        values.extend((0..100_000).map(|_| rng.next() as i64));
        for ms in values {
            assert_eq!(
                prom_timestamp(ms),
                prom_timestamp_before(ms),
                "prom_timestamp({ms})"
            );
        }
    }

    /// E3's strings: escapes, a line separator, two-byte and three-byte
    /// text, and keys that differ only past a shared prefix.
    const LABEL_STRINGS: &[&str] = &[
        "", "a", "a_b", "aB", "job", "__name__", "\"", "\\", "\u{1}", "\n", "\u{2028}", "é",
        "日本", "x\"y\\z",
    ];

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// **E3 (issue #795): the label object writes what it wrote before**,
    /// last-wins on a duplicate key included.
    #[test]
    fn labels_object_writes_what_it_wrote_before() {
        let mut inputs: Vec<Vec<(String, String)>> = vec![
            vec![],
            pairs(&[("zone", "1"), ("job", "api"), ("app", "x")]),
            pairs(&[("job", "first"), ("job", "second")]),
            pairs(&[("job", "second"), ("job", "first")]),
            pairs(&[("job", "a"), ("app", "b"), ("job", "c")]),
            pairs(&[("job", "api"), ("__name__", "up")]),
            pairs(&[("aB", "1"), ("a_b", "2"), ("a", "3")]),
        ];
        for &k in LABEL_STRINGS {
            for &v in LABEL_STRINGS {
                inputs.push(pairs(&[(k, v)]));
                inputs.push(pairs(&[("job", v), (k, "x")]));
            }
        }
        for labels in inputs {
            assert_eq!(
                labels_object_json(&labels),
                labels_object_json_before(&labels),
                "labels {labels:?}"
            );
        }
    }

    /// Up to four labels drawn from [`LABEL_STRINGS`].
    fn random_labels(rng: &mut XorShift) -> Vec<(String, String)> {
        (0..rng.below(5))
            .map(|_| {
                let k = LABEL_STRINGS[rng.below(LABEL_STRINGS.len() as u64)];
                let v = LABEL_STRINGS[rng.below(LABEL_STRINGS.len() as u64)];
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    /// A value from E1's generators: a named one, or a random bit pattern.
    fn random_value(rng: &mut XorShift) -> f64 {
        if rng.below(4) == 0 {
            PROM_FLOAT_NAMED[rng.below(PROM_FLOAT_NAMED.len() as u64)]
        } else {
            f64::from_bits(rng.next())
        }
    }

    /// A timestamp from E2's generators.
    fn random_ms(rng: &mut XorShift) -> i64 {
        if rng.below(2) == 0 {
            rng.below(6_001) as i64 - 3_000
        } else {
            rng.next() as i64
        }
    }

    fn hist(h: NativeHistogramParts) -> HistOrFloat {
        let (schema, zero_threshold, zero_count, count, sum, pos, neg, custom) = h;
        HistOrFloat::Hist(Box::new(
            pulsus_model::NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema,
                zero_threshold,
                zero_count,
                count,
                sum,
                positive_spans: pos.0,
                negative_spans: neg.0,
                positive_buckets: pos.1,
                negative_buckets: neg.1,
                custom_values: custom,
            }
            .to_float(),
        ))
    }

    type Buckets = (Vec<pulsus_model::Span>, Vec<i64>);
    type NativeHistogramParts = (i32, f64, u64, u64, f64, Buckets, Buckets, Vec<f64>);

    fn span(offset: i32, length: u32) -> pulsus_model::Span {
        pulsus_model::Span { offset, length }
    }

    /// `Hh1`, `Hh2`, `Hh3` of E4.
    fn e4_histograms() -> [HistOrFloat; 3] {
        [
            hist((
                0,
                0.001,
                2,
                9,
                -3.5,
                (vec![span(-2, 2)], vec![1, 2]),
                (vec![span(1, 2)], vec![2, -1]),
                vec![],
            )),
            hist((
                0,
                0.0,
                0,
                0,
                0.0,
                (vec![span(0, 2)], vec![0, 0]),
                (vec![], vec![]),
                vec![],
            )),
            hist((
                -53,
                0.0,
                0,
                5,
                4.25,
                (vec![span(0, 3)], vec![1, 0, 2]),
                (vec![], vec![]),
                vec![0.5, 1.0, 2.5],
            )),
        ]
    }

    fn random_hist_or_float(rng: &mut XorShift) -> HistOrFloat {
        let i = rng.below(4);
        if i == 3 {
            HistOrFloat::Float(random_value(rng))
        } else {
            e4_histograms()
                .into_iter()
                .nth(i)
                .expect("three histograms")
        }
    }

    /// The envelope from the `_before` renderers: the prefix, the items
    /// stable-sorted by labels and joined by `,`, then `]}}`.
    fn envelope_before<T>(
        result_type: &str,
        mut items: Vec<T>,
        labels: fn(&T) -> &Vec<(String, String)>,
        render: impl Fn(&T) -> Vec<u8>,
    ) -> String {
        items.sort_by(|a, b| labels(a).cmp(labels(b)));
        let rendered: Vec<String> = items
            .iter()
            .map(|i| String::from_utf8(render(i)).expect("utf8"))
            .collect();
        format!(
            "{{\"status\":\"success\",\"data\":{{\"resultType\":\"{result_type}\",\"result\":[{}]}}}}",
            rendered.join(",")
        )
    }

    /// **E4 (issue #795): the four query result kinds write what they
    /// wrote before**, `stream_array` included.
    #[tokio::test]
    async fn query_items_write_what_they_wrote_before() {
        let mut rng = XorShift::new();
        let matrix: Vec<MatrixSeries> = (0..200)
            .map(|_| MatrixSeries {
                labels: random_labels(&mut rng),
                points: (0..rng.below(9))
                    .map(|_| (random_ms(&mut rng), random_value(&mut rng)))
                    .collect(),
            })
            .collect();
        let vector: Vec<VectorSample> = (0..200)
            .map(|_| VectorSample {
                labels: random_labels(&mut rng),
                value: random_value(&mut rng),
            })
            .collect();
        let hist_matrix: Vec<HistMatrixSeries> = (0..50)
            .map(|_| HistMatrixSeries {
                labels: random_labels(&mut rng),
                points: (0..rng.below(9))
                    .map(|_| (random_ms(&mut rng), random_hist_or_float(&mut rng)))
                    .collect(),
            })
            .collect();
        let hist_vector: Vec<HistVectorSample> = (0..50)
            .map(|_| HistVectorSample {
                labels: random_labels(&mut rng),
                value: random_hist_or_float(&mut rng),
            })
            .collect();

        let cases = [
            (
                "matrix",
                query_response(QueryResult::Matrix(matrix.clone()), None, 0, false),
                envelope_before("matrix", matrix, |s| &s.labels, render_matrix_item_before),
            ),
            (
                "vector",
                query_response(QueryResult::Vector(vector.clone()), None, 5_500, false),
                envelope_before(
                    "vector",
                    vector,
                    |s| &s.labels,
                    |s| render_vector_item_before(s, 5_500),
                ),
            ),
            (
                "hist matrix",
                query_response(QueryResult::MatrixHist(hist_matrix.clone()), None, 0, false),
                envelope_before(
                    "matrix",
                    hist_matrix,
                    |s| &s.labels,
                    render_hist_matrix_item_before,
                ),
            ),
            (
                "hist vector",
                query_response(
                    QueryResult::VectorHist(hist_vector.clone()),
                    None,
                    5_500,
                    false,
                ),
                envelope_before(
                    "vector",
                    hist_vector,
                    |s| &s.labels,
                    |s| render_hist_vector_item_before(s, 5_500),
                ),
            ),
        ];
        for (kind, response, want) in cases {
            let got = body_string(response).await;
            assert_eq!(got, want, "{kind}: the body");
        }
    }

    /// Wire-format goldens captured from a real `prom/prometheus:v3.13.0`
    /// (issue #32 architect plan amendment §3, pinned mechanism —
    /// `tests/fixtures/prom_api/capture.sh`, provenance in that
    /// directory's `PROVENANCE.md`). Every fixture here was seeded at the
    /// **fixed** reference instant `1435781451` (`REF_MS` below), so these
    /// comparisons are reproducible independent of capture wall-clock time
    /// — the one exception (the float/special-value scalar goldens) reads
    /// its own capture-time timestamp back out of the fixture itself (see
    /// [`golden_scalar_at_ms`]) rather than asserting a wall-clock value.
    mod golden {
        use super::*;

        /// The fixed seed timestamp every non-scalar fixture in
        /// `tests/fixtures/prom_api/` was captured at (`capture.sh`'s
        /// `REF_TS=1435781451`, Prometheus's own documented API example
        /// timestamp), in milliseconds.
        const REF_MS: i64 = 1_435_781_451_000;

        async fn body_string(res: Response) -> String {
            let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
            String::from_utf8(bytes.to_vec()).expect("utf8")
        }

        /// Extracts a scalar query fixture's own capture-time evaluation
        /// timestamp (`data.result[0]`, a JSON number of unix seconds) and
        /// converts it back to milliseconds — these fixtures are captured
        /// live (`time` omitted, defaulting to "now" at capture), so their
        /// timestamp is not reproducible across captures; replaying the
        /// *same* fixture's own timestamp back through our encoder is what
        /// makes the byte-exact comparison meaningful regardless of when
        /// the fixture was last captured.
        fn golden_scalar_at_ms(fixture: &str) -> i64 {
            let json: serde_json::Value = serde_json::from_str(fixture).expect("valid json");
            let secs = json["data"]["result"][0]
                .as_f64()
                .expect("result[0] is a JSON number");
            (secs * 1000.0).round() as i64
        }

        fn up_vector() -> QueryResult {
            QueryResult::Vector(vec![
                VectorSample {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    value: 1.0,
                },
                VectorSample {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    value: 0.0,
                },
            ])
        }

        #[tokio::test]
        async fn query_vector_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/query.vector_get.json");
            let res = query_response(up_vector(), None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_matrix_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/query_range.matrix_get.json");
            let result = QueryResult::Matrix(vec![
                MatrixSeries {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 1.0)],
                },
                MatrixSeries {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 0.0)],
                },
            ]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        // --- issue #37: `__name__` keep/drop rule per construct class —
        // captured on both `query` and `query_range` per the bug's AC.
        // See PROVENANCE.md's "`__name__` keep/drop rule per construct
        // class" table for the full, interactively-verified matrix; these
        // three cover exactly the classes the bug named (selector/
        // aggregation/rate). ---

        #[tokio::test]
        async fn query_name_selector_keeps_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_selector_keeps_get.json");
            let res = query_response(up_vector(), None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_aggregation_drops_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_aggregation_drops_get.json");
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_rate_drops_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_rate_drops_get.json");
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("code".to_string(), "200".to_string()),
                    ("job".to_string(), "api".to_string()),
                    ("method".to_string(), "get".to_string()),
                ],
                value: 0.45,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_selector_keeps_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_selector_keeps_get.json"
            );
            let result = QueryResult::Matrix(vec![
                MatrixSeries {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 1.0)],
                },
                MatrixSeries {
                    labels: vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 0.0)],
                },
            ]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_aggregation_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_aggregation_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![],
                points: vec![(REF_MS, 1.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_rate_drops_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query_range.name_rate_drops_get.json");
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("code".to_string(), "200".to_string()),
                    ("job".to_string(), "api".to_string()),
                    ("method".to_string(), "get".to_string()),
                ],
                points: vec![(REF_MS, 0.45)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        // --- issue #37 code-review round: the AC-gap goldens (finding 4)
        // — topk/bottomk/*_over_time/histogram_quantile/binop, both
        // endpoints, byte-exact against the same pinned Prometheus
        // digest. See PROVENANCE.md's keep/drop table. ---

        #[tokio::test]
        async fn query_name_topk_keeps_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_topk_keeps_get.json");
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_topk_keeps_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query_range.name_topk_keeps_get.json");
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                points: vec![(REF_MS, 1.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_bottomk_keeps_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_bottomk_keeps_get.json");
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9101".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                value: 0.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_bottomk_keeps_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_bottomk_keeps_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9101".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                points: vec![(REF_MS, 0.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_over_time_drops_matches_captured() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query.name_over_time_drops_get.json");
            let result = QueryResult::Vector(vec![
                VectorSample {
                    labels: vec![
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    value: 1.0,
                },
                VectorSample {
                    labels: vec![
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    value: 0.0,
                },
            ]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_over_time_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_over_time_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![
                MatrixSeries {
                    labels: vec![
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 1.0)],
                },
                MatrixSeries {
                    labels: vec![
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    points: vec![(REF_MS, 0.0)],
                },
            ]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_histogram_quantile_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_histogram_quantile_drops_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![],
                value: 0.5,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_histogram_quantile_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_histogram_quantile_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![],
                points: vec![(REF_MS, 0.5)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_binop_arithmetic_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_binop_arithmetic_drops_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                value: 2.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_binop_arithmetic_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_binop_arithmetic_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                points: vec![(REF_MS, 2.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_comparison_plain_keeps_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_comparison_plain_keeps_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_comparison_plain_keeps_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_comparison_plain_keeps_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("__name__".to_string(), "up".to_string()),
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                points: vec![(REF_MS, 1.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_comparison_on_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_comparison_on_drops_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![("job".to_string(), "node".to_string())],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_comparison_on_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_comparison_on_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![("job".to_string(), "node".to_string())],
                points: vec![(REF_MS, 1.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_name_comparison_bool_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_comparison_bool_drops_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_range_name_comparison_bool_drops_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query_range.name_comparison_bool_drops_get.json"
            );
            let result = QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![
                    ("instance".to_string(), "localhost:9100".to_string()),
                    ("job".to_string(), "node".to_string()),
                ],
                points: vec![(REF_MS, 1.0)],
            }]);
            let res = query_response(result, None, 0, false);
            assert_eq!(body_string(res).await, fixture);
        }

        /// Issue #37 code-review round 3 [medium]: `on(__name__)` compares
        /// the *actual* metric name, not an empty/always-equal key — same
        /// name pairs and the result carries the real `__name__` (here,
        /// `on(__name__)` lists only `__name__`, so the ordinary matching
        /// key is empty and the sole output label is `__name__` itself).
        #[tokio::test]
        async fn query_name_on_dunder_name_same_name_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_on_dunder_name_same_name_matches_get.json"
            );
            let result = QueryResult::Vector(vec![VectorSample {
                labels: vec![("__name__".to_string(), "up".to_string())],
                value: 1.0,
            }]);
            let res = query_response(result, None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        /// Companion to the above: `on(__name__)` between two *different*
        /// metric names (`up` vs. `up_alias`) must not pair at all, so the
        /// result is an empty vector — never an empty-key false match.
        #[tokio::test]
        async fn query_name_on_dunder_name_different_names_empty_matches_captured() {
            let fixture = include_str!(
                "../../tests/fixtures/prom_api/query.name_on_dunder_name_different_names_empty_get.json"
            );
            let res = query_response(QueryResult::Vector(vec![]), None, REF_MS, false);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn labels_with_match_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/labels.with_match_get.json");
            let res = string_array_response(
                vec![
                    "__name__".to_string(),
                    "instance".to_string(),
                    "job".to_string(),
                ],
                None,
            );
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn label_values_job_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/label_values.job_get.json");
            let res = string_array_response(vec!["api".to_string(), "node".to_string()], None);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn series_with_match_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/series.with_match_get.json");
            let res = series_response(
                vec![
                    vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                ],
                None,
            );
            assert_eq!(body_string(res).await, fixture);
        }

        /// Issue #89: the regex-`__name__` discovery result's wire golden
        /// — `match[]={__name__=~"up.*"}` resolves both `up` series and
        /// `up_alias`, the discovery analog of #85's query-path regex-name
        /// resolution. Byte-exact against the pinned Prometheus capture.
        #[tokio::test]
        async fn series_name_regex_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/series.name_regex_get.json");
            let res = series_response(
                vec![
                    vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    vec![
                        ("__name__".to_string(), "up".to_string()),
                        ("instance".to_string(), "localhost:9101".to_string()),
                        ("job".to_string(), "node".to_string()),
                    ],
                    vec![
                        ("__name__".to_string(), "up_alias".to_string()),
                        ("instance".to_string(), "localhost:9100".to_string()),
                    ],
                ],
                None,
            );
            assert_eq!(body_string(res).await, fixture);
        }

        /// One test per captured float/special-value golden — the crux of
        /// the architect plan amendment §2's AC ("wire-format tests ...
        /// incl. float formatting and special-value strings"), and the
        /// mechanism that caught the `'g'`-vs-`'f'` deviation documented on
        /// [`prom_float`] and in this module's own doc comment.
        macro_rules! scalar_golden {
            ($test_name:ident, $fixture:literal, $value:expr) => {
                #[tokio::test]
                async fn $test_name() {
                    let fixture = include_str!(concat!("../../tests/fixtures/prom_api/", $fixture));
                    let at_ms = golden_scalar_at_ms(fixture);
                    let res = query_response(QueryResult::Scalar($value), None, at_ms, false);
                    assert_eq!(body_string(res).await, fixture);
                }
            };
        }

        scalar_golden!(
            query_scalar_zero_matches_captured,
            "query.scalar_zero.json",
            0.0
        );
        scalar_golden!(
            query_scalar_neg_zero_matches_captured,
            "query.scalar_neg_zero.json",
            -0.0
        );
        scalar_golden!(
            query_scalar_one_matches_captured,
            "query.scalar_one.json",
            1.0
        );
        scalar_golden!(
            query_scalar_100000_matches_captured,
            "query.scalar_100000.json",
            100_000.0
        );
        scalar_golden!(
            query_scalar_1e20_matches_captured,
            "query.scalar_1e20.json",
            1e20
        );
        scalar_golden!(
            query_scalar_1e21_matches_captured,
            "query.scalar_1e21.json",
            1e21
        );
        scalar_golden!(
            query_scalar_1e_minus_4_matches_captured,
            "query.scalar_1e_minus_4.json",
            1e-4
        );
        scalar_golden!(
            query_scalar_1e_minus_5_matches_captured,
            "query.scalar_1e_minus_5.json",
            1e-5
        );
        scalar_golden!(
            query_scalar_5e_minus_324_matches_captured,
            "query.scalar_5e_minus_324.json",
            5e-324
        );
        scalar_golden!(
            query_scalar_f64_max_matches_captured,
            "query.scalar_f64_max.json",
            f64::MAX
        );
        scalar_golden!(
            query_scalar_nan_matches_captured,
            "query.scalar_nan.json",
            f64::NAN
        );
        scalar_golden!(
            query_scalar_pos_inf_matches_captured,
            "query.scalar_pos_inf.json",
            f64::INFINITY
        );
        scalar_golden!(
            query_scalar_neg_inf_matches_captured,
            "query.scalar_neg_inf.json",
            f64::NEG_INFINITY
        );

        /// `status/tsdb`/`status/runtimeinfo` are capture-time-relative on
        /// real Prometheus (see PROVENANCE.md's determinism caveat) — this
        /// asserts our *own* envelope shape parses and carries the
        /// documented sub-fields, not byte-equality against the fixture.
        #[tokio::test]
        async fn status_tsdb_envelope_shape_matches_the_documented_sub_fields() {
            let fixture = include_str!("../../tests/fixtures/prom_api/status.tsdb_get.json");
            let captured: serde_json::Value = serde_json::from_str(fixture).expect("valid json");
            assert!(captured["data"]["headStats"]["numSeries"].is_u64());
            assert!(captured["data"]["seriesCountByMetricName"].is_array());

            let status = TsdbStatus {
                num_series: 1,
                series_count_by_metric_name: vec![("up".to_string(), 1)],
            };
            let res = status_tsdb_response(status);
            let ours: serde_json::Value =
                serde_json::from_str(&body_string(res).await).expect("valid json");
            assert!(ours["data"]["headStats"]["numSeries"].is_u64());
            // Code-review round-1 fix: `numSamples` is deliberately
            // omitted (not a real Prometheus `headStats` field; serving
            // it required a live ClickHouse query, violating the
            // zero-ClickHouse contract).
            assert!(ours["data"]["headStats"].get("numSamples").is_none());
            assert!(ours["data"]["seriesCountByMetricName"].is_array());
        }
    }
}
