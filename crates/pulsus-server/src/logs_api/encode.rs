//! The streaming JSON envelope encoder for `/api/logs/v1`'s five endpoints
//! (docs/api.md §2). Builds the response body incrementally from the
//! already-materialized `QueryResult`/`PlanExplain`/`Vec<String>` — never a
//! `serde_json::Value` DOM over the whole result set, never a second full
//! copy (issue #13 architect plan: "the streaming encoder writes the
//! response body incrementally ... no serde_json DOM for result sets").
//!
//! [`stream_array`] is the one low-level primitive every response shape
//! below is built from: it yields `prefix`, then one rendered chunk per
//! item (comma-separated), then `suffix`, via `futures::stream::unfold` —
//! at most one item's rendered bytes are ever alive between successive
//! `poll_next` calls (the encoder-memory AC amendment 1 exists to satisfy;
//! see this module's tests for the chunk-boundedness proof).
//!
//! **Poll-after-end (issue #24):** the raw `unfold` stream is `.fuse()`d
//! before it is handed to `Body::from_stream`. `Unfold`'s documented
//! invariant is that it must never be polled again once it has returned
//! `Poll::Ready(None)` — it panics otherwise. Under identity encoding,
//! axum/hyper never poll a body again after `None`, so the bug lay
//! dormant; `tower_http::compression::CompressionLayer`'s gzip encoder
//! polls the wrapped body once more past its final `None` to observe
//! EOF/flush, which re-polled the bare `Unfold` and panicked the request
//! task on every gzip-negotiated request. `Fuse` makes the extra poll a
//! safe no-op (`Poll::Ready(None)` forever) without buffering or changing
//! any frame this encoder yields.
//!
//! **Ordering (edge case #1):** the engine's results arrive in
//! `HashMap`-iteration order (unstable). Every response shape here sorts
//! its items by label set (streams: `(labels_json, fingerprint)`; matrix/
//! vector/series: the label vector itself) before framing, so wire output
//! is deterministic and golden fixtures are byte-exact.

use axum::body::{Body, Bytes};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Serialize;

use pulsus_read::{
    ExplainStage, MatrixSeries, PlanExplain, QueryResult, RouteChoice, StreamResult, VectorSample,
};

/// Builds a streaming JSON body: `prefix`, then `render(item)` for each
/// item in `items` (comma-separated), then `suffix`. `items` (the already-
/// materialized, O(limit)-bounded domain data) is moved into the stream's
/// state and lives for the whole drain — only the *current* item's
/// `render()` output is additional, temporary encoder memory.
///
/// The `unfold` stream is `.fuse()`d before reaching `Body::from_stream`
/// (issue #24): `Fuse` adds no buffering, it only turns a poll after the
/// stream's first `None` into another safe `None` instead of a panic —
/// load-bearing for `tower_http::compression::CompressionLayer`'s gzip
/// encoder, which polls once past EOF.
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

// ---------------------------------------------------------------------
// Small, fixed-size metadata blocks (stats/explain): `serde_json` is fine
// here — these are never the (potentially large) result array itself.
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct StreamStats {
    streams: usize,
    entries: usize,
    bytes: usize,
}

#[derive(Serialize)]
struct SeriesStats {
    series: usize,
}

#[derive(Serialize)]
struct ExplainWire<'a> {
    result_type: &'a str,
    routing: Option<RoutingWire<'a>>,
    stages: Vec<StageWire<'a>>,
}

#[derive(Serialize)]
struct RoutingWire<'a> {
    chosen: &'static str,
    reason: &'a str,
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
        routing: e.routing.as_ref().map(|r| RoutingWire {
            chosen: match r.chosen {
                RouteChoice::Rollup => "rollup",
                RouteChoice::Raw => "raw",
            },
            reason: &r.reason,
        }),
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

fn labels_object_json(labels: &[(String, String)]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".to_string())
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// Prometheus-style `unix_seconds.millis` timestamp (a bare JSON number
/// literal, embedded unquoted).
fn format_unix_seconds(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let millis = ns.rem_euclid(1_000_000_000) / 1_000_000;
    format!("{secs}.{millis:03}")
}

/// Prometheus-style sample value formatting: `NaN`/`+Inf`/`-Inf` as
/// strings, everything else via Rust's round-trip `f64` `Display` — always
/// returned as a **quoted** JSON string (docs/api.md §3.1's convention,
/// applied consistently here).
fn format_value_json(v: f64) -> String {
    let text = if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    };
    json_string(&text)
}

fn render_entries(entries: &[(i64, String)]) -> String {
    let mut out = String::new();
    for (i, (ts, line)) in entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("[\"{ts}\",{}]", json_string(line)));
    }
    out
}

fn render_stream_item(s: &StreamResult) -> Vec<u8> {
    format!(
        "{{\"stream\":{},\"values\":[{}]}}",
        s.labels_json,
        render_entries(&s.entries)
    )
    .into_bytes()
}

fn render_matrix_item(s: &MatrixSeries) -> Vec<u8> {
    let mut points = String::new();
    for (i, (step_ns, value)) in s.points.iter().enumerate() {
        if i > 0 {
            points.push(',');
        }
        points.push_str(&format!(
            "[{},{}]",
            format_unix_seconds(*step_ns),
            format_value_json(*value)
        ));
    }
    format!(
        "{{\"metric\":{},\"values\":[{}]}}",
        labels_object_json(&s.labels),
        points
    )
    .into_bytes()
}

fn render_vector_item(s: &VectorSample, at_ns: i64) -> Vec<u8> {
    format!(
        "{{\"metric\":{},\"value\":[{},{}]}}",
        labels_object_json(&s.labels),
        format_unix_seconds(at_ns),
        format_value_json(s.value)
    )
    .into_bytes()
}

fn explain_suffix(mut suffix: String, explain: Option<&PlanExplain>) -> String {
    if let Some(e) = explain {
        suffix.push_str(",\"explain\":");
        suffix.push_str(&explain_json(e));
    }
    suffix
}

/// Encodes a `query`/`query_range` result: `data.resultType`/`result`/
/// `stats`(/`explain`) per docs/api.md §2.1/§2.2. `at_ns` is the instant
/// evaluation time (`/query`'s `time` param) — only read when `result` is
/// [`QueryResult::Vector`] (never produced by a `Range` spec, so
/// `query_range` callers may pass any placeholder).
pub(crate) fn query_response(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ns: i64,
) -> Response {
    match result {
        QueryResult::Streams(mut items) => {
            items.sort_by(|a, b| {
                (&a.labels_json, a.fingerprint).cmp(&(&b.labels_json, b.fingerprint))
            });
            let stats = StreamStats {
                streams: items.len(),
                entries: items.iter().map(|s| s.entries.len()).sum(),
                bytes: items
                    .iter()
                    .flat_map(|s| s.entries.iter())
                    .map(|(_, line)| line.len())
                    .sum(),
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"streams\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
            let suffix = format!("{suffix}}}}}").into_bytes();
            json_response(stream_array(prefix, items, render_stream_item, suffix))
        }
        QueryResult::Matrix(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let stats = SeriesStats {
                series: items.len(),
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
            let suffix = format!("{suffix}}}}}").into_bytes();
            json_response(stream_array(prefix, items, render_matrix_item, suffix))
        }
        QueryResult::Vector(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let stats = SeriesStats {
                series: items.len(),
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
            let suffix = format!("{suffix}}}}}").into_bytes();
            json_response(stream_array(
                prefix,
                items,
                move |s: &VectorSample| render_vector_item(s, at_ns),
                suffix,
            ))
        }
        // Issue #31: `pulsus_promql::QueryValue::Scalar` (a bare-number
        // PromQL expression, e.g. `1 + 1`) — docs/api.md §2.1's documented
        // `"resultType":"scalar"` shape. No streaming needed (a single
        // value, unlike the O(series) result arrays above); `pulsus-server`
        // does not yet wire `MetricsEngine` into a route (that is #32), so
        // this arm is unreachable from any request today, but keeps
        // `QueryResult` matches exhaustive and correct for when it lands.
        QueryResult::Scalar(v) => {
            let body = explain_suffix(
                format!(
                    "{{\"status\":\"success\",\"data\":{{\"resultType\":\"scalar\",\"result\":[{},{}]}}",
                    format_unix_seconds(at_ns),
                    format_value_json(v)
                ),
                explain.as_ref(),
            );
            json_response(Body::from(format!("{body}}}")))
        }
        // Unreachable: `QueryResult::String` is a PromQL-only variant of
        // the shared enum (issue #86; a top-level string-literal metrics
        // query) — LogQL has no string result type at all. Kept as a
        // well-formed error response rather than a panic, mirroring
        // `prom_api::encode`'s own handling of the LogQL-only
        // `QueryResult::Streams` variant.
        QueryResult::String(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "status": "error",
                "errorType": "internal",
                "error": "unexpected string result from a logs query",
            })),
        )
            .into_response(),
    }
}

/// Encodes a `labels`/`label/{name}/values` result: `{"status":"success",
/// "data":["name1",...]}`, `explain` as a top-level sibling of `data` when
/// requested (docs/api.md §2.3).
pub(crate) fn string_array_response(items: Vec<String>, explain: Option<PlanExplain>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        items,
        |s: &String| json_string(s).into_bytes(),
        suffix,
    ))
}

/// Encodes a `series` result: `{"status":"success","data":[{k:v...},...]}`.
/// `items` are already-canonical label-set JSON object strings (from
/// `LogQlEngine::series`) — spliced verbatim, never re-parsed/re-encoded
/// (matches `pulsus-read::exec`'s own "never re-encode a response" design
/// note).
pub(crate) fn json_array_response(items: Vec<String>, explain: Option<PlanExplain>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        items,
        |s: &String| s.clone().into_bytes(),
        suffix,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read;

    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::routing::get;
    use flate2::read::GzDecoder;
    use pulsus_read::RoutingDecision;
    use tower::ServiceExt;
    use tower_http::compression::CompressionLayer;

    async fn body_string(res: Response) -> String {
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    fn stream(fp: u64, labels_json: &str, entries: Vec<(i64, &str)>) -> StreamResult {
        StreamResult {
            fingerprint: fp,
            service: "checkout".to_string(),
            labels_json: labels_json.to_string(),
            entries: entries
                .into_iter()
                .map(|(ts, line)| (ts, line.to_string()))
                .collect(),
        }
    }

    #[tokio::test]
    async fn streams_envelope_is_byte_exact_for_a_single_stream() {
        let result = QueryResult::Streams(vec![stream(
            1,
            r#"{"env":"prod","service_name":"checkout"}"#,
            vec![(100, "hello"), (200, "world")],
        )]);
        let res = query_response(result, None, 0);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"streams","result":[{"stream":{"env":"prod","service_name":"checkout"},"values":[["100","hello"],["200","world"]]}],"stats":{"streams":1,"entries":2,"bytes":10}}}"#
        );
    }

    #[tokio::test]
    async fn streams_envelope_sorts_multiple_streams_by_label_set_deterministically() {
        let result = QueryResult::Streams(vec![
            stream(2, r#"{"service_name":"zeta"}"#, vec![(1, "z")]),
            stream(1, r#"{"service_name":"alpha"}"#, vec![(1, "a")]),
        ]);
        let res = query_response(result, None, 0);
        let body = body_string(res).await;
        // "alpha" sorts before "zeta" lexicographically.
        let alpha_pos = body.find("alpha").expect("alpha present");
        let zeta_pos = body.find("zeta").expect("zeta present");
        assert!(alpha_pos < zeta_pos);
    }

    #[tokio::test]
    async fn streams_envelope_respects_the_global_limit_across_multiple_streams() {
        // Amendment 2's semantic pin: `limit` bounds total entries across
        // the whole response, not per stream. This fixture proves the
        // encoder faithfully reports whatever total the engine already
        // capped to (2 entries total across 2 streams), not a per-stream
        // count.
        let result = QueryResult::Streams(vec![
            stream(1, r#"{"service_name":"a"}"#, vec![(1, "x")]),
            stream(2, r#"{"service_name":"b"}"#, vec![(1, "y")]),
        ]);
        let res = query_response(result, None, 0);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["stats"]["entries"], 2);
        assert_eq!(json["data"]["stats"]["streams"], 2);
    }

    #[tokio::test]
    async fn streams_envelope_carries_data_explain_when_requested() {
        let mut explain = PlanExplain::new("streams");
        explain.push("stage1_stream_resolution", "SELECT 1", None);
        let result = QueryResult::Streams(vec![]);
        let res = query_response(result, Some(explain), 0);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["explain"]["result_type"], "streams");
        assert_eq!(
            json["data"]["explain"]["stages"][0]["name"],
            "stage1_stream_resolution"
        );
        assert!(json["data"]["explain"]["routing"].is_null());
    }

    #[tokio::test]
    async fn matrix_envelope_renders_points_and_series_stats() {
        let series = MatrixSeries {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(0, 1.0), (1_000_000_000, 2.5)],
        };
        let mut explain = PlanExplain::new("matrix");
        explain.set_routing(RoutingDecision {
            chosen: RouteChoice::Rollup,
            reason: "rollup: step divisible by resolution".to_string(),
        });
        let res = query_response(QueryResult::Matrix(vec![series]), Some(explain), 0);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["resultType"], "matrix");
        assert_eq!(json["data"]["stats"]["series"], 1);
        assert_eq!(
            json["data"]["result"][0]["metric"]["service_name"],
            "checkout"
        );
        assert_eq!(json["data"]["result"][0]["values"][0][0], 0.0);
        assert_eq!(json["data"]["result"][0]["values"][0][1], "1");
        assert_eq!(json["data"]["explain"]["routing"]["chosen"], "rollup");
    }

    #[tokio::test]
    async fn vector_envelope_uses_the_instant_evaluation_time() {
        let sample = VectorSample {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            value: 42.0,
        };
        let res = query_response(QueryResult::Vector(vec![sample]), None, 5_500_000_000);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["resultType"], "vector");
        assert_eq!(json["data"]["result"][0]["value"][0], 5.500);
        assert_eq!(json["data"]["result"][0]["value"][1], "42");
    }

    /// Byte-exact matrix golden (round-1 code-review finding 4a; finding 3
    /// — "matrix timestamps should be ns-strings" — was reviewed and
    /// rejected by architect plan amendment 3 §3: api.md §2.1 pins
    /// Prometheus-style `[<unix_seconds>, "<value>"]` matrix/vector points,
    /// distinct from streams' ns-string log-line timestamps. This fixture
    /// locks that exact wire shape, including the millisecond-resolution
    /// `.000`/`.500` formatting `format_unix_seconds` produces.
    #[tokio::test]
    async fn matrix_envelope_is_byte_exact_for_a_single_series() {
        let series = MatrixSeries {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(0, 1.0), (1_000_000_000, 2.5)],
        };
        let res = query_response(QueryResult::Matrix(vec![series]), None, 0);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"service_name":"checkout"},"values":[[0.000,"1"],[1.000,"2.5"]]}],"stats":{"series":1}}}"#
        );
    }

    /// Byte-exact vector golden (round-1 code-review finding 4b) — same
    /// Prometheus-style `[<unix_seconds>, "<value>"]` point shape as
    /// matrix, at the single instant-evaluation timestamp.
    #[tokio::test]
    async fn vector_envelope_is_byte_exact_for_a_single_sample() {
        let sample = VectorSample {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            value: 42.0,
        };
        let res = query_response(QueryResult::Vector(vec![sample]), None, 5_500_000_000);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"service_name":"checkout"},"value":[5.500,"42"]}],"stats":{"series":1}}}"#
        );
    }

    #[tokio::test]
    async fn empty_streams_result_still_renders_a_well_formed_envelope() {
        let res = query_response(QueryResult::Streams(vec![]), None, 0);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["result"], serde_json::json!([]));
        assert_eq!(json["data"]["stats"]["streams"], 0);
    }

    #[tokio::test]
    async fn string_array_envelope_escapes_values_and_supports_explain() {
        let mut explain = PlanExplain::new("labels");
        explain.push("label_names", "SELECT DISTINCT key", None);
        let res = string_array_response(
            vec!["env".to_string(), "with \"quote\"".to_string()],
            Some(explain),
        );
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"][0], "env");
        assert_eq!(json["data"][1], "with \"quote\"");
        assert_eq!(json["explain"]["result_type"], "labels");
    }

    #[tokio::test]
    async fn json_array_envelope_splices_canonical_labels_json_verbatim() {
        let res = json_array_response(
            vec![r#"{"env":"prod","service_name":"checkout"}"#.to_string()],
            None,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"env":"prod","service_name":"checkout"}]}"#
        );
    }

    /// Byte-exact series golden (round-1 code-review finding 4c) — multiple
    /// already-canonical label-object JSON strings, comma-joined verbatim
    /// with no re-parse/re-encode.
    #[tokio::test]
    async fn series_envelope_is_byte_exact_for_multiple_label_sets() {
        let res = json_array_response(
            vec![
                r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                r#"{"env":"staging","service_name":"checkout"}"#.to_string(),
            ],
            None,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"env":"prod","service_name":"checkout"},{"env":"staging","service_name":"checkout"}]}"#
        );
    }

    #[tokio::test]
    async fn empty_array_response_renders_an_empty_data_array() {
        let res = string_array_response(vec![], None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":[]}"#);
    }

    /// Encoder memory bound (architect plan amendment 1, encoder unit test
    /// 2(a)): drives a synthetic 100k-entry streams result (spread across
    /// many small streams, the worst case for "one item at a time")
    /// through the raw chunk stream and asserts every individual yielded
    /// chunk stays near one stream's own size — never anywhere close to
    /// the full ~100k-entry aggregate. This is a stronger, more direct
    /// proof of "bounded intermediate buffering" than measuring process
    /// allocation would be: a chunk that is itself small **cannot** be
    /// the product of a whole-result `serde_json` DOM/second copy.
    #[tokio::test]
    async fn streams_encoder_yields_bounded_chunks_for_a_100k_entry_synthetic_result() {
        const NUM_STREAMS: usize = 1000;
        const ENTRIES_PER_STREAM: usize = 100; // 100_000 entries total.

        let items: Vec<StreamResult> = (0..NUM_STREAMS)
            .map(|i| {
                let labels_json = format!(r#"{{"service_name":"svc-{i:05}"}}"#);
                let entries = (0..ENTRIES_PER_STREAM)
                    .map(|j| {
                        (
                            (i * ENTRIES_PER_STREAM + j) as i64,
                            "a modestly sized log line for chunk-bound measurement purposes"
                                .to_string(),
                        )
                    })
                    .collect();
                StreamResult {
                    fingerprint: i as u64,
                    service: "checkout".to_string(),
                    labels_json,
                    entries,
                }
            })
            .collect();

        let res = query_response(QueryResult::Streams(items), None, 0);
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

        // One prefix chunk + one chunk per stream + one suffix chunk.
        assert_eq!(chunk_count, NUM_STREAMS + 2);
        // Total output is large (~100k entries' worth of text) ...
        assert!(total_len > 5_000_000, "total_len = {total_len}");
        // ... but no single chunk is anywhere near that size: each stream
        // item's own chunk (100 entries of ~70 bytes) is a few KB, so a
        // generous 64KB ceiling is still two orders of magnitude below the
        // aggregate — proving the encoder never materializes the whole
        // result as one buffer.
        assert!(
            max_chunk_len < 64 * 1024,
            "max_chunk_len = {max_chunk_len} (aggregate would be ~{total_len})"
        );
    }

    /// Poll-after-end regression (issue #24): `futures::stream::Unfold`'s
    /// documented contract is that it must never be polled again once it
    /// has returned `Poll::Ready(None)` — it `panic!`s otherwise. This is
    /// exactly what `tower_http::compression::CompressionLayer`'s gzip
    /// encoder does (it polls the wrapped body once more past EOF), which
    /// used to abort the request task on every gzip-negotiated request.
    /// Drives the body's data stream to completion, then polls it once
    /// more, and asserts a second, safe `None` — the minimal reproduction
    /// of the defect, with no compression dependency needed. Fails (panics
    /// at `unfold.rs:108`) on `Body::from_stream(stream)` without `.fuse()`.
    #[tokio::test]
    async fn stream_array_body_yields_none_instead_of_panicking_when_polled_after_completion() {
        let result = QueryResult::Streams(vec![stream(
            1,
            r#"{"service_name":"checkout"}"#,
            vec![(100, "hello")],
        )]);
        let res = query_response(result, None, 0);
        let mut body_stream = res.into_body().into_data_stream();

        while body_stream.next().await.is_some() {}

        assert!(
            body_stream.next().await.is_none(),
            "polling the body stream once more after completion must yield None, not panic"
        );
    }

    /// Runs `build` (a response-shape constructor) through a real
    /// `CompressionLayer`-wrapped router twice — once with no
    /// `Accept-Encoding` (identity) and once with `Accept-Encoding: gzip`
    /// — and asserts the gzip-decoded body is byte-identical to the
    /// identity body. Exercises the actual layer that triggers the
    /// poll-after-end panic (a synthetic `unfold`/`.fuse()` test cannot
    /// prove the *real* compression encoder is satisfied).
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
    async fn gzip_streams_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let result = QueryResult::Streams(vec![
                stream(
                    1,
                    r#"{"env":"prod","service_name":"checkout"}"#,
                    vec![(100, "hello"), (200, "world")],
                ),
                stream(
                    2,
                    r#"{"env":"staging","service_name":"checkout"}"#,
                    vec![(150, "another line")],
                ),
            ]);
            query_response(result, None, 0)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_empty_streams_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            query_response(QueryResult::Streams(vec![]), None, 0)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_matrix_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let series = MatrixSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: vec![(0, 1.0), (1_000_000_000, 2.5)],
            };
            query_response(QueryResult::Matrix(vec![series]), None, 0)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_vector_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let sample = VectorSample {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                value: 42.0,
            };
            query_response(QueryResult::Vector(vec![sample]), None, 5_500_000_000)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_string_array_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            string_array_response(vec!["env".to_string(), "service_name".to_string()], None)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_series_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            json_array_response(
                vec![
                    r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                    r#"{"env":"staging","service_name":"checkout"}"#.to_string(),
                ],
                None,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }
}
