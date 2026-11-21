//! `POST /v1development/profiles` through its handler, over a mock sink:
//! T1 to T10 and J1, J2. No ClickHouse.

use std::io::Write;
use std::sync::Mutex;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use prost::Message;

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Link, Location, Mapping, Profile, ProfilesDictionary,
    ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use pulsus_model::Tenant;
use pulsus_write::{
    AdmitRefusal, FlushWait, ParsedProfileLanding, ProfileSink, PushHeaders, ingest_otlp_profiles,
};

const VERSION: &str = "otlp-profiles-development-version";
const OLD: &str = " A request without OTLP-Profiles-Development-Version is read as development version 1. Send the header, or use an OpenTelemetry Collector at v0.148.0 or later.";
const H4: &str = "a sample with attributes and timestamps but no values is refused without OTLP-Profiles-Development-Version, because earlier versions encode values in that field";
/// The cap the server is given by default, 256 MiB.
const CAP: u64 = 268_435_456;

const P: [u8; 16] = [
    0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98, 0xa9, 0xba, 0xcb, 0xdc, 0xed, 0xfe, 0x0f,
];

// -- the mock sink ------------------------------------------------------

struct Call {
    tenant: String,
    landing: ParsedProfileLanding,
    /// `true` when `admit_flush` took it, `false` for `admit`.
    flushed: bool,
}

/// Records each call; answers `Ok`, or `refusal` when set.
#[derive(Default)]
struct Mock {
    refusal: Option<AdmitRefusal>,
    calls: Mutex<Vec<Call>>,
}

impl Mock {
    fn record(&self, tenant: &Tenant, landing: ParsedProfileLanding, flushed: bool) {
        self.calls.lock().unwrap().push(Call {
            tenant: tenant.as_str().to_owned(),
            landing,
            flushed,
        });
    }

    fn count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }

    fn landings(&self) -> Vec<ParsedProfileLanding> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.landing.clone())
            .collect()
    }
}

impl ProfileSink for Mock {
    fn admit(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        _push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        self.record(tenant, landing, false);
        match self.refusal {
            Some(r) => Err(r),
            None => Ok(()),
        }
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        _push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        self.record(tenant, landing, true);
        match self.refusal {
            Some(r) => Err(r),
            None => Ok(FlushWait::new(async { Ok(()) })),
        }
    }
}

// -- requests -----------------------------------------------------------

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Answer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// The `google.rpc.Status` body's code and message.
    fn status_body(&self) -> (i32, String) {
        let s = Status::decode(self.body.as_slice())
            .unwrap_or_else(|e| panic!("a google.rpc.Status body ({e}): {:?}", self.text()));
        (s.code, s.message)
    }

    /// Asserts the plain-text error container and its exact body.
    fn assert_plain(&self, status: StatusCode, body: &str) {
        assert_eq!(self.status, status, "{:?}", self.text());
        assert_eq!(
            self.header("content-type"),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(self.header("x-content-type-options"), Some("nosniff"));
        assert_eq!(self.text(), body);
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

async fn send_capped(mock: &Mock, headers: &[(&str, &str)], body: Vec<u8>, cap: u64) -> Answer {
    let mut map = HeaderMap::new();
    for (k, v) in headers {
        map.append(
            HeaderName::from_bytes(k.as_bytes()).expect("a header name"),
            HeaderValue::from_str(v).expect("a header value"),
        );
    }
    let res = ingest_otlp_profiles(mock, map, Body::from(body), cap).await;
    let status = res.status();
    let headers = res.headers().clone();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("the body")
        .to_vec();
    Answer {
        status,
        headers,
        body,
    }
}

async fn send(mock: &Mock, headers: &[(&str, &str)], body: Vec<u8>) -> Answer {
    send_capped(mock, headers, body, CAP).await
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as u64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_owned())),
        }),
        ..KeyValue::default()
    }
}

fn st(table: &[&str]) -> Vec<String> {
    table.iter().map(|s| (*s).to_owned()).collect()
}

fn line(function_index: i32) -> Location {
    Location {
        lines: vec![Line {
            function_index,
            ..Line::default()
        }],
        ..Location::default()
    }
}

/// §0's request, timed `time_ns`.
fn r_at(time_ns: u64) -> ExportProfilesServiceRequest {
    ExportProfilesServiceRequest {
        dictionary: Some(ProfilesDictionary {
            string_table: st(&[
                "",
                "samples",
                "count",
                "cpu",
                "nanoseconds",
                "main.work",
                "runtime.mallocgc",
            ]),
            mapping_table: vec![Mapping::default()],
            function_table: vec![
                Function::default(),
                Function {
                    name_strindex: 5,
                    ..Function::default()
                },
                Function {
                    name_strindex: 6,
                    ..Function::default()
                },
            ],
            location_table: vec![Location::default(), line(1), line(2)],
            stack_table: vec![
                Stack::default(),
                Stack {
                    location_indices: vec![1],
                },
                Stack {
                    location_indices: vec![2, 1],
                },
            ],
            link_table: vec![
                Link::default(),
                Link {
                    trace_id: vec![0xaa; 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                },
            ],
            attribute_table: vec![KeyValueAndUnit::default()],
        }),
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout"),
                    string_kv("host.name", "h1"),
                ],
                ..Resource::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                profiles: vec![Profile {
                    sample_type: Some(ValueType {
                        type_strindex: 1,
                        unit_strindex: 2,
                    }),
                    period_type: Some(ValueType {
                        type_strindex: 3,
                        unit_strindex: 4,
                    }),
                    period: 10_000_000,
                    time_unix_nano: time_ns,
                    duration_nano: 10_000_000_000,
                    profile_id: P.to_vec(),
                    samples: vec![
                        Sample {
                            stack_index: 1,
                            values: vec![3],
                            ..Sample::default()
                        },
                        Sample {
                            stack_index: 2,
                            values: vec![1],
                            link_index: 1,
                            ..Sample::default()
                        },
                    ],
                    ..Profile::default()
                }],
                ..ScopeProfiles::default()
            }],
            ..ResourceProfiles::default()
        }],
    }
}

/// `R`: §0's request timed now.
fn r() -> ExportProfilesServiceRequest {
    r_at(now_ns())
}

fn r_bytes() -> Vec<u8> {
    r().encode_to_vec()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(bytes).expect("gzip");
    gz.finish().expect("gzip")
}

// -- hand-encoded old samples --------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
    put_varint(out, u64::from(field) << 3);
    put_varint(out, v);
}

fn ld_field(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(out, (u64::from(field) << 3) | 2);
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

fn packed_varints(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        put_varint(&mut out, *v);
    }
    out
}

fn packed_fixed64(values: &[u64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// An old-layout sample on stack 1 (field 1), then `rest`.
fn old_sample(rest: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::new();
    varint_field(&mut out, 1, 1);
    rest(&mut out);
    out
}

/// `D`, the header-less fixture dictionary.
fn d() -> ProfilesDictionary {
    ProfilesDictionary {
        string_table: st(&["", "samples", "count", "cpu", "nanoseconds", "f", "k", "v"]),
        attribute_table: vec![
            KeyValueAndUnit::default(),
            KeyValueAndUnit {
                key_strindex: 6,
                value: Some(AnyValue {
                    value: Some(Value::StringValue("v".to_owned())),
                }),
                unit_strindex: 0,
            },
        ],
        function_table: vec![
            Function::default(),
            Function {
                name_strindex: 5,
                ..Function::default()
            },
        ],
        location_table: vec![Location::default(), line(1)],
        mapping_table: vec![Mapping::default()],
        stack_table: vec![
            Stack::default(),
            Stack {
                location_indices: vec![1],
            },
        ],
        link_table: vec![Link::default()],
    }
}

/// A sample-less version 1 profile, `samples:count` with a period of one
/// `cpu:nanoseconds`, timed `time_ns`.
fn sampleless(time_ns: u64) -> Profile {
    Profile {
        sample_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        period_type: Some(ValueType {
            type_strindex: 3,
            unit_strindex: 4,
        }),
        period: 1,
        time_unix_nano: time_ns,
        ..Profile::default()
    }
}

/// A request with `D` whose one profile's bytes are `profile`.
fn wrap(profile: &[u8]) -> Vec<u8> {
    let mut scope = Vec::new();
    ld_field(&mut scope, 2, profile);
    let mut resource = Vec::new();
    ld_field(
        &mut resource,
        1,
        &Resource {
            attributes: vec![string_kv("service.name", "svc")],
            ..Resource::default()
        }
        .encode_to_vec(),
    );
    ld_field(&mut resource, 2, &scope);
    let mut out = Vec::new();
    ld_field(&mut out, 1, &resource);
    ld_field(&mut out, 2, &d().encode_to_vec());
    out
}

/// A request with `D` whose one profile is [`sampleless`] with each of
/// `samples` appended as field 2, then `tail`.
fn with_old_samples(samples: &[Vec<u8>], tail: &[u8]) -> Vec<u8> {
    let mut profile = sampleless(now_ns()).encode_to_vec();
    for s in samples {
        ld_field(&mut profile, 2, s);
    }
    profile.extend_from_slice(tail);
    wrap(&profile)
}

/// The timestamps-only old sample: two timestamps in field 5.
fn timestamps_only() -> Vec<u8> {
    let now = now_ns();
    old_sample(|o| ld_field(o, 5, &packed_fixed64(&[now, now])))
}

fn values_of(mock: &Mock) -> Vec<i64> {
    mock.landings()
        .iter()
        .flat_map(|l| l.samples.iter().map(|s| s.value))
        .collect()
}

// -- T1 to T10 ------------------------------------------------------------

/// **T1.** `OTLP-Profiles-Development-Version`: absent and `1` are read,
/// anything else is refused before the body is read.
#[tokio::test]
async fn t1_the_version_header() {
    let mock = Mock::default();
    assert_eq!(send(&mock, &[], r_bytes()).await.status, StatusCode::OK);
    assert_eq!(
        send(&mock, &[(VERSION, "1")], r_bytes()).await.status,
        StatusCode::OK
    );
    for (headers, message) in [
        (
            vec![(VERSION, "2")],
            "unsupported OTLP-Profiles-Development-Version 2: this server reads version 1",
        ),
        (
            vec![(VERSION, "01")],
            "malformed OTLP-Profiles-Development-Version \"01\": a positive integer without leading zeros is required",
        ),
        (
            vec![(VERSION, "1"), (VERSION, "1")],
            "OTLP-Profiles-Development-Version must be sent once; it was sent 2 times",
        ),
    ] {
        let a = send(&mock, &headers, r_bytes()).await;
        assert_eq!(a.status, StatusCode::BAD_REQUEST, "{headers:?}");
        assert_eq!(a.status_body(), (3, message.to_owned()), "{headers:?}");
    }
    assert_eq!(mock.count(), 2, "only absent and `1` are taken");

    let a = send(&mock, &[(VERSION, "2")], vec![0xff, 0xff]).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        a.status_body(),
        (
            3,
            "unsupported OTLP-Profiles-Development-Version 2: this server reads version 1"
                .to_owned()
        )
    );
}

/// **T2.** The tenant is read from `X-Scope-OrgID`; an invalid one stores
/// nothing.
#[tokio::test]
async fn t2_the_tenant() {
    let mock = Mock::default();
    let a = send(&mock, &[("x-scope-orgid", "t1")], r_bytes()).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(mock.calls.lock().unwrap()[0].tenant, "t1");

    let mock = Mock::default();
    let a = send(&mock, &[("x-scope-orgid", "a|b")], r_bytes()).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST);
    assert_eq!(a.status_body(), (3, "invalid X-Scope-OrgID".to_owned()));
    assert_eq!(mock.count(), 0);
}

/// **T3.** Success: `200` after the flush, `202` under `X-Pulsus-Async: 1`,
/// an empty protobuf answer; no samples is a success with nothing admitted.
#[tokio::test]
async fn t3_success_sync_async_and_empty() {
    let mock = Mock::default();
    let a = send(&mock, &[], r_bytes()).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(a.header("content-type"), Some("application/x-protobuf"));
    assert!(a.body.is_empty(), "{:?}", a.body);
    assert!(mock.calls.lock().unwrap()[0].flushed);

    let a = send(&mock, &[("x-pulsus-async", "1")], r_bytes()).await;
    assert_eq!(a.status, StatusCode::ACCEPTED, "{:?}", a.text());
    assert_eq!(a.header("content-type"), Some("application/x-protobuf"));
    assert!(a.body.is_empty());
    assert!(!mock.calls.lock().unwrap()[1].flushed);

    let mock = Mock::default();
    let mut empty = r();
    empty.resource_profiles[0].scope_profiles[0].profiles[0]
        .samples
        .clear();
    let a = send(&mock, &[], empty.encode_to_vec()).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(mock.count(), 0, "nothing admitted");
}

/// **T4.** Without the version header, a sample in an earlier layout is
/// refused rather than stored as something else.
#[tokio::test]
async fn t4_old_samples_without_the_header() {
    let now = now_ns();
    let mock = Mock::default();
    let refusals: [(Vec<u8>, String); 5] = [
        (
            old_sample(|o| ld_field(o, 3, &packed_varints(&[1]))),
            format!(
                "OTLP profiles: Sample field 3 has wire type 2; development version 1 sends wire type 0.{OLD}"
            ),
        ),
        (
            old_sample(|o| varint_field(o, 4, 1)),
            format!(
                "OTLP profiles: Sample field 4 has wire type 0; development version 1 sends wire type 2.{OLD}"
            ),
        ),
        (
            old_sample(|o| ld_field(o, 2, &packed_varints(&[5]))),
            format!("failed to convert otel profile: sample value is required{OLD}"),
        ),
        (
            old_sample(|o| {
                ld_field(o, 2, &packed_varints(&[5]));
                ld_field(o, 5, &packed_fixed64(&[now]));
            }),
            format!(
                "failed to convert otel profile: could not access attribute at index 0: index 5 out of bounds{OLD}"
            ),
        ),
        (
            old_sample(|o| {
                ld_field(o, 2, &packed_varints(&[1]));
                ld_field(o, 5, &packed_fixed64(&[now]));
            }),
            format!("failed to convert otel profile: {H4}{OLD}"),
        ),
    ];
    for (i, (sample, message)) in refusals.into_iter().enumerate() {
        let a = send(&mock, &[], with_old_samples(&[sample], &[])).await;
        assert_eq!(
            a.status,
            StatusCode::BAD_REQUEST,
            "case {i}: {:?}",
            a.text()
        );
        assert_eq!(a.status_body(), (3, message), "case {i}");
    }
    assert_eq!(mock.count(), 0);

    let a = send(&mock, &[], with_old_samples(&[timestamps_only()], &[])).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(mock.count(), 1);
    assert_eq!(values_of(&mock), [2]);
}

/// **T5.** Without the header a field version 1 does not define, or one
/// of another wire type, is refused, as is a request with no dictionary;
/// with the header the first is read.
#[tokio::test]
async fn t5_old_profile_fields_and_no_dictionary() {
    let mock = Mock::default();
    let mut field12 = Vec::new();
    ld_field(&mut field12, 12, &packed_varints(&[1]));
    let unknown = with_old_samples(&[timestamps_only()], &field12);

    let a = send(&mock, &[], unknown.clone()).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{:?}", a.text());
    assert_eq!(
        a.status_body(),
        (
            3,
            format!(
                "OTLP profiles: Profile field 12 is not defined in development version 1.{OLD}"
            )
        )
    );

    let mut profile = sampleless(0).encode_to_vec();
    ld_field(&mut profile, 2, &timestamps_only());
    ld_field(&mut profile, 3, &packed_fixed64(&[1, 2]));
    let a = send(&mock, &[], wrap(&profile)).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{:?}", a.text());
    assert_eq!(
        a.status_body(),
        (
            3,
            format!(
                "OTLP profiles: Profile field 3 has wire type 2; development version 1 sends wire type 1.{OLD}"
            )
        )
    );

    let mut no_dictionary = r();
    no_dictionary.dictionary = None;
    let a = send(&mock, &[], no_dictionary.encode_to_vec()).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{:?}", a.text());
    assert_eq!(
        a.status_body(),
        (3, "missing profile metadata dictionary".to_owned())
    );
    assert_eq!(mock.count(), 0);

    let a = send(&mock, &[(VERSION, "1")], unknown).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(values_of(&mock), [2]);
}

/// **T6.** With the header, attributes and timestamps without values are
/// version 1's own shape and are read.
#[tokio::test]
async fn t6_attributes_and_timestamps_with_the_header() {
    let now = now_ns();
    let mock = Mock::default();
    let sample = old_sample(|o| {
        ld_field(o, 2, &packed_varints(&[1]));
        ld_field(o, 5, &packed_fixed64(&[now]));
    });
    let a = send(&mock, &[(VERSION, "1")], with_old_samples(&[sample], &[])).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(values_of(&mock), [1]);
    let landing = &mock.landings()[0];
    assert!(
        landing.series[0].labels.contains(r#""k":"v""#),
        "{}",
        landing.series[0].labels
    );
}

/// **T7.** The model's own refusal answers `400`.
#[tokio::test]
async fn t7_a_profile_outside_the_window_is_refused() {
    let mock = Mock::default();
    let old = r_at(now_ns() - 2 * 3_600 * 1_000_000_000).encode_to_vec();
    let a = send(&mock, &[], old).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{:?}", a.text());
    let (code, message) = a.status_body();
    assert_eq!(code, 3);
    assert!(
        message.contains("is outside of ingestion window"),
        "{message}"
    );
    assert_eq!(mock.count(), 0);
}

/// **T8.** Backpressure answers `429`, code 8.
#[tokio::test]
async fn t8_backpressure() {
    let mock = Mock {
        refusal: Some(AdmitRefusal::Backpressure),
        ..Mock::default()
    };
    let a = send(&mock, &[], r_bytes()).await;
    assert_eq!(a.status, StatusCode::TOO_MANY_REQUESTS, "{:?}", a.text());
    assert_eq!(a.status_body().0, 8);
}

/// **T9.** The body cap, on the body as read and as decompressed: `413` in
/// the reference's words. `0` is no cap.
#[tokio::test]
async fn t9_the_body_cap() {
    let mock = Mock::default();
    let a = send_capped(&mock, &[], vec![0x0a; 1_001], 1_000).await;
    a.assert_plain(
        StatusCode::PAYLOAD_TOO_LARGE,
        "profile payload size exceeds limit of 1.0 kB\n",
    );

    let inflating = gzip(&[0u8; 1_001]);
    assert!(inflating.len() < 1_000);
    let a = send_capped(&mock, &[("content-encoding", "gzip")], inflating, 1_000).await;
    a.assert_plain(
        StatusCode::PAYLOAD_TOO_LARGE,
        "uncompressed profile payload size exceeds limit of 1.0 kB\n",
    );

    let a = send_capped(&mock, &[], vec![0xff; 1_000], 1_000).await;
    a.assert_plain(
        StatusCode::BAD_REQUEST,
        "Failed to parse protobuf request\n",
    );
    assert_eq!(mock.count(), 0);

    assert_eq!(
        pulsus_write::ingest::http::humanize_bytes(268_435_456),
        "268 MB"
    );
    assert_eq!(pulsus_write::ingest::http::humanize_bytes(1_000), "1.0 kB");

    // The cap off: a gzip body inflating past the fixed 64 MiB cap the
    // other routes keep is decoded, not refused for its size, and `R` is
    // stored.
    let big = gzip(&vec![0u8; 64 * 1024 * 1024 + 1]);
    let a = send_capped(&mock, &[("content-encoding", "gzip")], big, 0).await;
    a.assert_plain(
        StatusCode::BAD_REQUEST,
        "Failed to parse protobuf request\n",
    );
    let a = send_capped(&mock, &[("content-encoding", "gzip")], gzip(&r_bytes()), 0).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(mock.count(), 1);
}

/// **T10.** A body that does not decode answers the reference's plain text.
#[tokio::test]
async fn t10_an_undecodable_body_is_plain_text() {
    let mock = Mock::default();
    let a = send(&mock, &[], vec![0xff, 0xff]).await;
    a.assert_plain(
        StatusCode::BAD_REQUEST,
        "Failed to parse protobuf request\n",
    );
    let a = send(
        &mock,
        &[("content-type", "application/json")],
        b"{".to_vec(),
    )
    .await;
    a.assert_plain(StatusCode::BAD_REQUEST, "Failed to parse JSON request\n");
    assert_eq!(mock.count(), 0);
}

// -- J1, J2 ---------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `R` as OTLP/JSON, timed `time_ns`, with `samples_key` naming the
/// profile's samples.
fn r_json(time_ns: u64, samples_key: &str) -> Vec<u8> {
    format!(
        r#"{{
  "resourceProfiles": [{{
    "resource": {{"attributes": [
      {{"key": "service.name", "value": {{"stringValue": "checkout"}}}},
      {{"key": "host.name", "value": {{"stringValue": "h1"}}}}
    ]}},
    "scopeProfiles": [{{"profiles": [{{
      "sampleType": {{"typeStrindex": 1, "unitStrindex": 2}},
      "periodType": {{"typeStrindex": 3, "unitStrindex": 4}},
      "period": "10000000",
      "timeUnixNano": "{time_ns}",
      "durationNano": "10000000000",
      "profileId": "{profile_id}",
      "{samples_key}": [
        {{"stackIndex": 1, "values": ["3"]}},
        {{"stackIndex": 2, "values": ["1"], "linkIndex": 1}}
      ]
    }}]}}]
  }}],
  "dictionary": {{
    "stringTable": ["", "samples", "count", "cpu", "nanoseconds", "main.work", "runtime.mallocgc"],
    "mappingTable": [{{}}],
    "functionTable": [{{}}, {{"nameStrindex": 5}}, {{"nameStrindex": 6}}],
    "locationTable": [{{}}, {{"lines": [{{"functionIndex": 1}}]}}, {{"lines": [{{"functionIndex": 2}}]}}],
    "stackTable": [{{}}, {{"locationIndices": [1]}}, {{"locationIndices": [2, 1]}}],
    "linkTable": [{{}}, {{"traceId": "{trace_id}", "spanId": "0102030405060708"}}],
    "attributeTable": [{{}}]
  }}
}}"#,
        profile_id = hex(&P),
        trace_id = hex(&[0xaa; 16]),
    )
    .into_bytes()
}

/// **J1.** `R` as OTLP/JSON lands exactly as `R` as protobuf.
#[tokio::test]
async fn j1_json_lands_as_protobuf_does() {
    let time = now_ns();
    let mock = Mock::default();
    let a = send(&mock, &[], r_at(time).encode_to_vec()).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    let a = send(
        &mock,
        &[("content-type", "application/json")],
        r_json(time, "samples"),
    )
    .await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(a.header("content-type"), Some("application/x-protobuf"));
    let landings = mock.landings();
    assert_eq!(landings.len(), 2);
    assert!(!landings[0].samples.is_empty());
    assert_eq!(landings[0], landings[1]);
}

/// **J2.** Without the header a key version 1 does not define is refused;
/// with it, the key is ignored.
#[tokio::test]
async fn j2_an_old_json_key() {
    let mock = Mock::default();
    let json = ("content-type", "application/json");
    let a = send(&mock, &[json], r_json(now_ns(), "sample")).await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{:?}", a.text());
    let (code, message) = a.status_body();
    assert_eq!(code, 3);
    assert!(
        message.contains(r#"Profile has no field "sample""#) && message.ends_with(OLD),
        "{message}"
    );

    let a = send(&mock, &[json, (VERSION, "1")], r_json(now_ns(), "sample")).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    assert_eq!(mock.count(), 0);
}

/// **J4.** An attribute value given as a string-table index is resolved
/// against the dictionary on OTLP/JSON as on protobuf: the resource's
/// `service.name` and a sample's label both arrive as `stringValueStrindex`
/// and both are stored.
#[tokio::test]
async fn j4_string_table_values_are_resolved_on_json() {
    let time = now_ns();
    let strings = [
        "",
        "samples",
        "count",
        "cpu",
        "nanoseconds",
        "main.work",
        "runtime.mallocgc",
        "checkout",
        "region",
        "eu",
        "service.name",
    ];
    let indexed = |i: i32| AnyValue {
        value: Some(Value::StringValueStrindex(i)),
    };
    let mut proto = r_at(time);
    let dict = proto.dictionary.as_mut().expect("a dictionary");
    dict.string_table = st(&strings);
    dict.attribute_table.push(KeyValueAndUnit {
        key_strindex: 8,
        value: Some(indexed(9)),
        unit_strindex: 0,
    });
    proto.resource_profiles[0].resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_owned(),
            value: Some(indexed(7)),
            ..KeyValue::default()
        }],
        ..Resource::default()
    });
    proto.resource_profiles[0].scope_profiles[0].profiles[0].samples[0].attribute_indices = vec![1];

    let table: Vec<String> = strings.iter().map(|s| format!("\"{s}\"")).collect();
    let json = format!(
        r#"{{
  "resourceProfiles": [{{
    "resource": {{"attributes": [{{"key": "service.name", "value": {{"stringValueStrindex": 7}}}}]}},
    "scopeProfiles": [{{"profiles": [{{
      "sampleType": {{"typeStrindex": 1, "unitStrindex": 2}},
      "periodType": {{"typeStrindex": 3, "unitStrindex": 4}},
      "period": "10000000",
      "timeUnixNano": "{time}",
      "durationNano": "10000000000",
      "profileId": "{profile_id}",
      "samples": [
        {{"stackIndex": 1, "values": ["3"], "attributeIndices": [1]}},
        {{"stackIndex": 2, "values": ["1"], "linkIndex": 1}}
      ]
    }}]}}]
  }}],
  "dictionary": {{
    "stringTable": [{table}],
    "mappingTable": [{{}}],
    "functionTable": [{{}}, {{"nameStrindex": 5}}, {{"nameStrindex": 6}}],
    "locationTable": [{{}}, {{"lines": [{{"functionIndex": 1}}]}}, {{"lines": [{{"functionIndex": 2}}]}}],
    "stackTable": [{{}}, {{"locationIndices": [1]}}, {{"locationIndices": [2, 1]}}],
    "linkTable": [{{}}, {{"traceId": "{trace_id}", "spanId": "0102030405060708"}}],
    "attributeTable": [{{}}, {{"keyStrindex": 8, "value": {{"stringValueStrindex": 9}}}}]
  }}
}}"#,
        table = table.join(", "),
        profile_id = hex(&P),
        trace_id = hex(&[0xaa; 16]),
    );

    let mock = Mock::default();
    let a = send(&mock, &[], proto.encode_to_vec()).await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    let a = send(
        &mock,
        &[("content-type", "application/json")],
        json.into_bytes(),
    )
    .await;
    assert_eq!(a.status, StatusCode::OK, "{:?}", a.text());
    let landings = mock.landings();
    assert_eq!(landings.len(), 2);
    for (name, landing) in ["protobuf", "JSON"].iter().zip(&landings) {
        let services: Vec<&str> = landing.series.iter().map(|s| &*s.service).collect();
        assert!(
            services.iter().all(|s| *s == "checkout"),
            "{name}: {services:?}"
        );
        assert!(
            landing
                .series
                .iter()
                .any(|s| s.labels.contains(r#""region":"eu""#)),
            "{name}: {:?}",
            landing.series
        );
    }
    assert_eq!(landings[0], landings[1]);
}

/// #833: `docs/api.md` §1.1 quotes the crate's header-less note and names
/// the collector it names as the oldest supported one.
#[test]
fn the_api_reference_quotes_the_header_less_note_and_its_collector() {
    let docs = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/api.md"))
        .expect("read docs/api.md");
    let start = docs
        .find("### 1.1 OTLP (primary)")
        .expect("docs/api.md has §1.1");
    let end = start
        + docs[start..]
            .find("### 1.2")
            .expect("docs/api.md has §1.2 after §1.1");
    let section = &docs[start..end];

    let note = pulsus_write::protocols::otlp_prescan::PROFILES_HEADER_LESS_NOTE.trim();
    assert!(
        section.contains(&format!("`{note}`")),
        "docs/api.md §1.1 does not quote the header-less note `{note}`"
    );
    let v = note
        .split_once("Collector at ")
        .and_then(|(_, rest)| rest.split_once(" or later"))
        .map(|(v, _)| v)
        .expect("the note names a collector release");
    assert!(
        section.contains(&format!(
            "**The oldest supported OpenTelemetry Collector is {v}.**"
        )),
        "docs/api.md §1.1 does not name {v} as the oldest supported collector"
    );
    assert!(
        section
            .lines()
            .any(|line| line.starts_with(&format!("  | {v} to "))),
        "docs/api.md §1.1's sender table has no row starting at {v}"
    );
}
