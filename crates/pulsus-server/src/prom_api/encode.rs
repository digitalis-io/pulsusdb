//! The streaming JSON envelope encoder for `/api/v1/*` (docs/api.md §3).
//! Builds the response body incrementally from the already-materialized
//! `QueryResult`/`PlanExplain`/`Vec<String>`/discovery results — never a
//! `serde_json::Value` DOM over a result set (issue #32 architect plan,
//! mirroring `logs_api::encode`'s own contract).
//!
//! [`stream_array`] is a **self-contained copy** of `logs_api::encode`'s
//! own primitive (architect plan: `prom_api` is fully self-contained —
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
    ExplainStage, MatrixSeries, MetricMeta, PlanExplain, QueryResult, TsdbStatus, VectorSample,
};

use crate::app::BuildInfo;

/// Builds a streaming JSON body: `prefix`, then `render(item)` for each
/// item in `items` (comma-separated), then `suffix`. `items` (the already-
/// materialized, domain data) is moved into the stream's state and lives
/// for the whole drain — only the *current* item's `render()` output is
/// additional, temporary encoder memory. See the module doc for the
/// `.fuse()` rationale (issue #24).
fn stream_array<T, R>(prefix: Vec<u8>, items: Vec<T>, render: R, suffix: Vec<u8>) -> Body
where
    T: Send + 'static,
    R: Fn(&T) -> Vec<u8> + Send + 'static,
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
                let mut chunk = if i > 0 { vec![b','] } else { Vec::new() };
                chunk.extend((state.render)(&state.items[i]));
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

fn labels_object_json(labels: &[(String, String)]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".to_string())
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
/// This function therefore always renders `%f`-style via [`render_fixed`]
/// — never scientific notation — reproducing Go's shortest round-trip
/// decimal digits (via `format!("{v:e}")`'s mantissa) placed at the
/// correct decimal-point position for *every* finite magnitude, including
/// the `5e-324` subnormal boundary. Special values first (`NaN`/`+Inf`/
/// `-Inf` as literal strings, `-0`/`0` verbatim). Sample values are always
/// emitted as **quoted** JSON strings by the caller (Prometheus
/// convention), never bare numbers — this function itself returns the
/// unquoted text.
pub(crate) fn prom_float(v: f64) -> String {
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

    let body = render_fixed(&digits, exp);
    if negative { format!("-{body}") } else { body }
}

/// Go `%f`-style rendering: the decimal point sits `exp + 1` digits from
/// the left of `digits` (Rust's shortest-round-trip mantissa never carries
/// a trailing zero, so no trailing-zero trim is ever needed here).
fn render_fixed(digits: &str, exp: i32) -> String {
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

/// Prometheus-style timestamp (`jsonutil.MarshalTimestamp`): integer
/// seconds when `ms` lands exactly on a second boundary, else
/// `secs.fff`-with-trailing-zeros-trimmed — deliberately diverges from
/// `logs_api::encode::format_unix_seconds`'s always-`.fff` padding (see
/// the module doc). Emitted as a **bare** JSON number by the caller, never
/// a quoted string.
pub(crate) fn prom_timestamp(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    if millis == 0 {
        return secs.to_string();
    }
    let frac = format!("{millis:03}");
    let trimmed = frac.trim_end_matches('0');
    format!("{secs}.{trimmed}")
}

/// Sample values are always quoted JSON strings (docs/api.md §3.1).
fn quoted_value(v: f64) -> String {
    json_string(&prom_float(v))
}

/// The error envelope [`query_response`]'s unreachable `QueryResult::Streams`
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
// Explain (issue #32: adds `exactness`, a constant `"raw-exact"` in M2 —
// tiers/rollup routing are M3).
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct ExplainWire<'a> {
    result_type: &'a str,
    exactness: &'static str,
    stages: Vec<StageWire<'a>>,
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
        exactness: "raw-exact",
        stages: e
            .stages
            .iter()
            .map(|s: &ExplainStage| StageWire {
                name: s.name,
                sql: &s.sql,
                note: s.note.as_deref(),
            })
            .collect(),
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

fn render_matrix_item(s: &MatrixSeries) -> Vec<u8> {
    let mut points = String::new();
    for (i, (t_ms, value)) in s.points.iter().enumerate() {
        if i > 0 {
            points.push(',');
        }
        points.push_str(&format!(
            "[{},{}]",
            prom_timestamp(*t_ms),
            quoted_value(*value)
        ));
    }
    format!(
        "{{\"metric\":{},\"values\":[{}]}}",
        labels_object_json(&s.labels),
        points
    )
    .into_bytes()
}

fn render_vector_item(s: &VectorSample, at_ms: i64) -> Vec<u8> {
    format!(
        "{{\"metric\":{},\"value\":[{},{}]}}",
        labels_object_json(&s.labels),
        prom_timestamp(at_ms),
        quoted_value(s.value)
    )
    .into_bytes()
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
pub(crate) fn query_response(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ms: i64,
    ordered: bool,
) -> Response {
    match result {
        QueryResult::Vector(mut items) => {
            if !ordered {
                items.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}}}").into_bytes();
            json_response(stream_array(
                prefix,
                items,
                move |s: &VectorSample| render_vector_item(s, at_ms),
                suffix,
            ))
        }
        QueryResult::Matrix(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix("]".to_string(), explain.as_ref());
            let suffix = format!("{suffix}}}}}").into_bytes();
            json_response(stream_array(prefix, items, render_matrix_item, suffix))
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
            json_response(Body::from(format!("{data_body}}}}}")))
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
            json_response(Body::from(format!("{data_body}}}}}")))
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

/// Encodes a `labels`/`label/{name}/values` result: `{"status":"success",
/// "data":["name1",...]}`.
pub(crate) fn string_array_response(items: Vec<String>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = b"]}".to_vec();
    json_response(stream_array(
        prefix,
        items,
        |s: &String| json_string(s).into_bytes(),
        suffix,
    ))
}

/// Encodes a `series` result: `{"status":"success","data":[{k:v...},...]}`
/// — each item is a full label-pair vector, `__name__` already spliced in
/// by `MetricsEngine::series`.
pub(crate) fn series_response(items: Vec<Vec<(String, String)>>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = b"]}".to_vec();
    json_response(stream_array(
        prefix,
        items,
        |pairs: &Vec<(String, String)>| labels_object_json(pairs).into_bytes(),
        suffix,
    ))
}

// ---------------------------------------------------------------------
// metadata (issue #32) — Prometheus's own shape: a map keyed by metric
// name, each value an array of descriptor objects (always length 1 here:
// `metric_metadata` stores exactly one row per base family name).
// ---------------------------------------------------------------------

fn render_metadata_entry(m: &MetricMeta) -> Vec<u8> {
    format!(
        "{}:[{{\"type\":{},\"help\":{},\"unit\":{}}}]",
        json_string(&m.name),
        json_string(&m.metric_type),
        json_string(&m.help),
        json_string(&m.unit)
    )
    .into_bytes()
}

pub(crate) fn metadata_response(mut items: Vec<MetricMeta>) -> Response {
    items.sort_by(|a, b| a.name.cmp(&b.name));
    let prefix = b"{\"status\":\"success\",\"data\":{".to_vec();
    let suffix = b"}}".to_vec();
    json_response(stream_array(prefix, items, render_metadata_entry, suffix))
}

// ---------------------------------------------------------------------
// query_exemplars (issue #32: empty-success stub in M2).
// ---------------------------------------------------------------------

pub(crate) fn query_exemplars_response() -> Response {
    json_response(Body::from(r#"{"status":"success","data":[]}"#))
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
}

pub(crate) fn status_runtimeinfo_response(
    start_time_rfc3339: String,
    retention_days: u32,
) -> Response {
    success(RuntimeInfoWire {
        start_time: start_time_rfc3339,
        storage_retention: format!("{retention_days}d"),
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

    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::routing::get;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tower::ServiceExt;
    use tower_http::compression::CompressionLayer;

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
        let res = string_array_response(vec!["__name__".to_string(), "job".to_string()]);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":["__name__","job"]}"#);
    }

    #[tokio::test]
    async fn empty_string_array_response_renders_an_empty_data_array() {
        let res = string_array_response(vec![]);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":[]}"#);
    }

    #[tokio::test]
    async fn series_envelope_is_byte_exact_for_a_single_series() {
        let res = series_response(vec![vec![
            ("__name__".to_string(), "up".to_string()),
            ("job".to_string(), "api".to_string()),
        ]]);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"__name__":"up","job":"api"}]}"#
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

    // --- query_exemplars stub ---

    #[tokio::test]
    async fn query_exemplars_response_is_an_empty_success() {
        let res = query_exemplars_response();
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":[]}"#);
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
        let res = status_runtimeinfo_response("2026-01-01T00:00:00+00:00".to_string(), 7);
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
            .layer(CompressionLayer::new());

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
            series_response(vec![vec![("__name__".to_string(), "up".to_string())]])
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
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
            let res = string_array_response(vec![
                "__name__".to_string(),
                "instance".to_string(),
                "job".to_string(),
            ]);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn label_values_job_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/label_values.job_get.json");
            let res = string_array_response(vec!["api".to_string(), "node".to_string()]);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn series_with_match_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/series.with_match_get.json");
            let res = series_response(vec![
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
            ]);
            assert_eq!(body_string(res).await, fixture);
        }

        /// Issue #89: the regex-`__name__` discovery result's wire golden
        /// — `match[]={__name__=~"up.*"}` resolves both `up` series and
        /// `up_alias`, the discovery analog of #85's query-path regex-name
        /// resolution. Byte-exact against the pinned Prometheus capture.
        #[tokio::test]
        async fn series_name_regex_matches_the_captured_prometheus_response() {
            let fixture = include_str!("../../tests/fixtures/prom_api/series.name_regex_get.json");
            let res = series_response(vec![
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
            ]);
            assert_eq!(body_string(res).await, fixture);
        }

        #[tokio::test]
        async fn query_exemplars_matches_the_captured_prometheus_response() {
            let fixture =
                include_str!("../../tests/fixtures/prom_api/query_exemplars.empty_get.json");
            assert_eq!(body_string(query_exemplars_response()).await, fixture);
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
