//! The profile push receivers: `POST /push.v1.PusherService/Push`, a Connect
//! unary call carrying a `PushRequest`, and `POST /ingest`, the reference's
//! legacy route, of which the pprof path is served here. Each reads the
//! tenant first, reads the body under [`MAX_PROFILE_BODY_BYTES`], decodes and
//! validates every profile, and only then admits the push whole, the shape of
//! the other receivers in `ingest/http.rs`.

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::ingest::profiles::ProfileSink;

/// The bound on a request body as read, and on the push route's body after
/// gzip: the reference's `distributor.ingestion-body-limit-mb` default
/// (`pkg/validation/limits.go:24,164`).
pub const MAX_PROFILE_BODY_BYTES: usize = 268_435_456;

/// Stub.
pub async fn ingest_profile_push(
    _sink: &dyn ProfileSink,
    _headers: HeaderMap,
    _body: Body,
) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

/// Stub.
pub async fn ingest_profile_legacy(
    _sink: &dyn ProfileSink,
    _query: Option<&str>,
    _headers: HeaderMap,
    _body: Body,
) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::{RawQuery, State};
    use axum::routing::post;
    use http_body_util::BodyExt;
    use prost::Message;
    use pulsus_profiles_wire::push_v1::{PushRequest, RawProfileSeries, RawSample};
    use pulsus_profiles_wire::types_v1::LabelPair;
    use tower::ServiceExt;

    use super::*;
    use crate::error::LogsIngestError;
    use crate::ingest::profiles::ParsedProfileLanding;
    use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
    use crate::protocols::profile_push::fixture::{self, Pb};

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Mode {
        Admit,
        AdmitFlush,
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Ok,
        Backpressure,
        FlushFails,
    }

    /// `http.rs`'s `MockTraceSink`, for [`ProfileSink`]: records each landing
    /// and which admission took it.
    struct MockProfileSink {
        outcome: Outcome,
        admitted: Mutex<Vec<(ParsedProfileLanding, Mode)>>,
    }

    impl MockProfileSink {
        fn new(outcome: Outcome) -> Arc<MockProfileSink> {
            Arc::new(MockProfileSink {
                outcome,
                admitted: Mutex::new(Vec::new()),
            })
        }

        fn landings(&self) -> Vec<(ParsedProfileLanding, Mode)> {
            self.admitted.lock().unwrap().clone()
        }
    }

    impl ProfileSink for MockProfileSink {
        fn admit(
            &self,
            _tenant: &pulsus_model::Tenant,
            landing: ParsedProfileLanding,
            _push: PushHeaders,
        ) -> Result<(), AdmitRefusal> {
            self.admitted.lock().unwrap().push((landing, Mode::Admit));
            match self.outcome {
                Outcome::Backpressure => Err(AdmitRefusal::Backpressure),
                _ => Ok(()),
            }
        }

        fn admit_flush(
            &self,
            _tenant: &pulsus_model::Tenant,
            landing: ParsedProfileLanding,
            _push: PushHeaders,
        ) -> Result<FlushWait, AdmitRefusal> {
            self.admitted
                .lock()
                .unwrap()
                .push((landing, Mode::AdmitFlush));
            match self.outcome {
                Outcome::Backpressure => Err(AdmitRefusal::Backpressure),
                Outcome::FlushFails => Ok(FlushWait::new(async {
                    Err(LogsIngestError::FlushFailed("insert refused".to_string()))
                })),
                Outcome::Ok => Ok(FlushWait::new(async { Ok(()) })),
            }
        }
    }

    const PUSH: &str = "/push.v1.PusherService/Push";

    fn router(sink: Arc<MockProfileSink>) -> Router {
        async fn push(
            State(sink): State<Arc<MockProfileSink>>,
            headers: HeaderMap,
            body: Body,
        ) -> Response {
            ingest_profile_push(sink.as_ref(), headers, body).await
        }
        async fn legacy(
            State(sink): State<Arc<MockProfileSink>>,
            RawQuery(query): RawQuery,
            headers: HeaderMap,
            body: Body,
        ) -> Response {
            ingest_profile_legacy(sink.as_ref(), query.as_deref(), headers, body).await
        }
        Router::new()
            .route(PUSH, post(push))
            .route("/ingest", post(legacy))
            .with_state(sink)
    }

    struct Answer {
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    impl Answer {
        fn content_type(&self) -> Option<&str> {
            self.headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.body).into_owned()
        }
    }

    async fn send(
        sink: &Arc<MockProfileSink>,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Answer {
        let mut builder = axum::http::Request::builder().method("POST").uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let res = router(sink.clone())
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let body = res.into_body().collect().await.unwrap().to_bytes().to_vec();
        Answer {
            status,
            headers,
            body,
        }
    }

    fn label(name: &str, value: &str) -> LabelPair {
        LabelPair {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// §0's push: one series, one profile.
    fn push_request(raw_profile: Vec<u8>) -> PushRequest {
        PushRequest {
            series: vec![RawProfileSeries {
                labels: vec![
                    label("__name__", "process_cpu"),
                    label("service_name", "checkout"),
                    label("env", "prod"),
                ],
                samples: vec![RawSample {
                    raw_profile,
                    id: String::new(),
                }],
                annotations: Vec::new(),
            }],
        }
    }

    const PROTO: (&str, &str) = ("content-type", "application/proto");
    const JSON: (&str, &str) = ("content-type", "application/json");

    fn b64_std(bytes: &[u8]) -> String {
        const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = match chunk.len() {
                3 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32,
                2 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8,
                _ => (chunk[0] as u32) << 16,
            };
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    fn b64_url_unpadded(bytes: &[u8]) -> String {
        b64_std(bytes)
            .trim_end_matches('=')
            .replace('+', "-")
            .replace('/', "_")
    }

    fn json_push(field: &str, raw: &str) -> Vec<u8> {
        format!(
            r#"{{"series":[{{"labels":[{{"name":"__name__","value":"process_cpu"}},{{"name":"service_name","value":"checkout"}},{{"name":"env","value":"prod"}}],"samples":[{{"{field}":"{raw}","ID":""}}],"unknownField":1}}]}}"#
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn h1_the_push_route_admits_a_protobuf_push() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(fixture::pb()).encode_to_vec();
        let res = send(&sink, PUSH, &[PROTO, ("x-scope-orgid", "t1")], body).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert_eq!(res.content_type(), Some("application/proto"));
        assert!(res.body.is_empty());
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        assert_eq!(landings[0].1, Mode::AdmitFlush);
        assert_eq!(landings[0].0.series.len(), 2);
    }

    #[tokio::test]
    async fn h2_the_push_route_reads_proto3_json() {
        let pb = fixture::pb();
        let sink = MockProfileSink::new(Outcome::Ok);
        let proto = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(pb.clone()).encode_to_vec(),
        )
        .await;
        assert_eq!(proto.status, StatusCode::OK, "{}", proto.text());
        for (field, raw) in [
            ("rawProfile", b64_std(&pb)),
            ("raw_profile", b64_url_unpadded(&pb)),
        ] {
            let res = send(&sink, PUSH, &[JSON], json_push(field, &raw)).await;
            assert_eq!(res.status, StatusCode::OK, "{field}: {}", res.text());
            assert_eq!(res.content_type(), Some("application/json"), "{field}");
            assert_eq!(res.body, b"{}", "{field}");
        }
        let landings = sink.landings();
        assert_eq!(landings.len(), 3);
        assert_eq!(landings[1].0, landings[0].0, "rawProfile");
        assert_eq!(landings[2].0, landings[0].0, "raw_profile");
    }

    #[tokio::test]
    async fn h3_the_push_route_gunzips_the_body() {
        let pb = fixture::pb();
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(pb).encode_to_vec();
        let plain = send(&sink, PUSH, &[PROTO], body.clone()).await;
        assert_eq!(plain.status, StatusCode::OK, "{}", plain.text());
        let res = send(
            &sink,
            PUSH,
            &[PROTO, ("content-encoding", "gzip")],
            fixture::gzip(&body),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let landings = sink.landings();
        assert_eq!(landings.len(), 2);
        assert_eq!(landings[1].0, landings[0].0);
    }

    #[tokio::test]
    async fn h4_the_push_route_negotiates_as_connect_does() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(fixture::pb()).encode_to_vec();
        let res = send(
            &sink,
            PUSH,
            &[("content-type", "application/x-protobuf")],
            body.clone(),
        )
        .await;
        assert_eq!(res.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(res.body.is_empty());
        assert_eq!(
            res.headers.get("accept-post").and_then(|v| v.to_str().ok()),
            Some("application/json, application/proto")
        );
        let res = send(&sink, PUSH, &[PROTO, ("content-encoding", "zstd")], body).await;
        assert_eq!(res.status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(res.content_type(), Some("application/json"));
        assert_eq!(
            res.text(),
            r#"{"code":"unimplemented","message":"unknown compression \"zstd\": supported encodings are gzip"}"#
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h5_the_push_route_refuses_what_the_reference_refuses() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let big = fixture::gzip(&vec![0u8; 4_194_305]);
        let cases = [
            (PushRequest { series: Vec::new() }, "no profiles received"),
            (
                push_request(fixture::no_sample_types().encode_to_vec()),
                "empty profile",
            ),
            (
                push_request(big),
                "uncompressed profile payload size exceeds limit of 4.2 MB",
            ),
        ];
        for (req, message) in cases {
            let res = send(&sink, PUSH, &[PROTO], req.encode_to_vec()).await;
            assert_eq!(res.status, StatusCode::BAD_REQUEST, "{message}");
            assert_eq!(res.content_type(), Some("application/json"), "{message}");
            assert_eq!(
                res.text(),
                format!(r#"{{"code":"invalid_argument","message":"{message}"}}"#)
            );
        }
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h6_both_routes_read_the_tenant_first() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let tenant = ("x-scope-orgid", "a|b");
        let res = send(
            &sink,
            PUSH,
            &[PROTO, tenant],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            r#"{"code":"invalid_argument","message":"invalid X-Scope-OrgID"}"#
        );
        let res = send(
            &sink,
            "/ingest?name=app&format=pprof",
            &[tenant],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            "{\"code\":\"invalid_argument\",\"message\":\"invalid X-Scope-OrgID\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h7_backpressure_is_resource_exhausted_on_both_routes() {
        let sink = MockProfileSink::new(Outcome::Backpressure);
        let message = "sink is applying backpressure: buffers are full";
        let res = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            res.text(),
            format!(r#"{{"code":"resource_exhausted","message":"{message}"}}"#)
        );
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            res.text(),
            format!("{{\"code\":\"resource_exhausted\",\"message\":\"{message}\"}}\n")
        );
        assert_eq!(
            res.headers
                .get("x-content-type-options")
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        assert_eq!(res.content_type(), Some("application/json"));
    }

    #[tokio::test]
    async fn h8_ingest_names_the_series_by_the_whole_name() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(
            &sink,
            "/ingest?name=app.cpu%7Benv%3Dprod%7D&format=pprof",
            &[],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(res.body.is_empty());
        assert_eq!(res.content_type(), None);
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        assert_eq!(
            landings[0].0.series[0].labels,
            r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:samples:count:cpu:nanoseconds","__service_name__":"app.cpu","__type__":"samples","__unit__":"count","env":"prod","pyroscope_spy":"unknown","service_name":"app.cpu"}"#
        );
    }

    fn lock_profile() -> Vec<u8> {
        let mut b = Pb::new(fixture::now_ns());
        b.sample_type("contentions", "count")
            .sample_type("delay", "nanoseconds")
            .period("contentions", "count")
            .func(1, "main", "main.go")
            .loc(1, &[1])
            .sample(&[1], &[2, 2_000]);
        b.build().encode_to_vec()
    }

    #[tokio::test]
    async fn h9_ingest_names_the_series_through_the_sample_type_config() {
        let config = br#"{"contentions":{"display-name":"block_count"},"delay":{"display-name":"block_duration"}}"#;
        let profile = lock_profile();
        let ct = ("content-type", "multipart/form-data; boundary=b0undary");
        let cases: [(Option<&[u8]>, [&str; 2]); 2] = [
            (
                Some(config),
                [
                    "block:contentions:count:contentions:count",
                    "block:delay:nanoseconds:contentions:count",
                ],
            ),
            (
                None,
                [
                    "mutex:contentions:count:contentions:count",
                    "mutex:delay:nanoseconds:contentions:count",
                ],
            ),
        ];
        for (config, want) in cases {
            let sink = MockProfileSink::new(Outcome::Ok);
            let mut parts: Vec<(&str, Option<&str>, &[u8])> =
                vec![("profile", Some("profile.pprof"), &profile)];
            if let Some(c) = config {
                parts.push(("sample_type_config", Some("config.json"), c));
            }
            let body = fixture::multipart("b0undary", &parts);
            let res = send(&sink, "/ingest?name=app&spyName=gospy", &[ct], body).await;
            assert_eq!(res.status, StatusCode::OK, "{}", res.text());
            let landings = sink.landings();
            assert_eq!(landings.len(), 1);
            let type_ids: Vec<&str> = landings[0].0.series.iter().map(|s| &*s.type_id).collect();
            assert_eq!(type_ids, want, "config {}", config.is_some());
        }
    }

    #[tokio::test]
    async fn h10_ingest_without_a_name_is_refused() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            "{\"code\":\"invalid_argument\",\"message\":\"name: service name is required\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h11_ingest_refusals_are_unprocessable() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let uri = "/ingest?name=app&format=pprof";
        let res = send(&sink, uri, &[], fixture::no_sample_types().encode_to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"pushing IngestInput-pprof failed push series with index 0 and id  failed: invalid_argument: empty profile\"}\n"
        );
        let res = send(&sink, uri, &[], b"garbage".to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            res.text()
                .starts_with("{\"code\":\"unknown\",\"message\":\"parsing IngestInput-pprof failed invalid_argument: "),
            "{}",
            res.text()
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h12_ingest_without_a_profile_stores_nothing() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], Vec::new()).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let body = fixture::multipart("b0undary", &[("other", Some("x"), b"x")]);
        let res = send(
            &sink,
            "/ingest?name=app",
            &[("content-type", "multipart/form-data; boundary=b0undary")],
            body,
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h13_ingest_refuses_the_formats_not_served_yet() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(
            &sink,
            "/ingest?name=app",
            &[("content-type", "text/plain")],
            b"main;a 1\n".to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"format groups is not supported\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h14_async_admits_without_waiting() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let asynch = ("x-pulsus-async", "1");
        let res = send(
            &sink,
            "/ingest?name=app&format=pprof",
            &[asynch],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::ACCEPTED, "{}", res.text());
        let res = send(
            &sink,
            PUSH,
            &[PROTO, asynch],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let modes: Vec<Mode> = sink.landings().iter().map(|(_, m)| *m).collect();
        assert_eq!(modes, [Mode::Admit, Mode::Admit]);
    }

    #[tokio::test]
    async fn h15_a_failed_insert_is_a_server_error() {
        let sink = MockProfileSink::new(Outcome::FlushFails);
        let res = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            res.text().starts_with(r#"{"code":"internal","message":""#),
            "{}",
            res.text()
        );
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            res.text().starts_with(r#"{"code":"unknown","message":""#),
            "{}",
            res.text()
        );
    }
}
