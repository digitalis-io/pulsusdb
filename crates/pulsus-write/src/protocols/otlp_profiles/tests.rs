//! M1 to M9: an OTLP profiles request converted to the profile model's
//! inputs, and those inputs parsed to rows.

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue,
};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Link, Location, Mapping, Profile, ProfilesDictionary,
    ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use super::to_inputs;
use crate::ingest::profiles::{LandingStack, ParsedProfileLanding, ProfileInput};
use crate::protocols::profile_model::parse_profiles;

const P: [u8; 16] = [
    0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98, 0xa9, 0xba, 0xcb, 0xdc, 0xed, 0xfe, 0x0f,
];
const SPAN: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
const TRACE: [u8; 16] = [0xaa; 16];

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .expect("a representable time")
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

/// A dictionary attribute keyed by string-table index `key` with `value`.
fn attr(key: i32, value: Value) -> KeyValueAndUnit {
    KeyValueAndUnit {
        key_strindex: key,
        value: Some(AnyValue { value: Some(value) }),
        unit_strindex: 0,
    }
}

fn st(table: &[&str]) -> Vec<String> {
    table.iter().map(|s| (*s).to_owned()).collect()
}

/// §0's dictionary.
fn dictionary() -> ProfilesDictionary {
    ProfilesDictionary {
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
        location_table: vec![
            Location::default(),
            Location {
                lines: vec![Line {
                    function_index: 1,
                    ..Line::default()
                }],
                ..Location::default()
            },
            Location {
                lines: vec![Line {
                    function_index: 2,
                    ..Line::default()
                }],
                ..Location::default()
            },
        ],
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
                trace_id: TRACE.to_vec(),
                span_id: SPAN.to_vec(),
            },
        ],
        attribute_table: vec![KeyValueAndUnit::default()],
    }
}

/// §0's profile at `time_ns`.
fn profile(time_ns: u64) -> Profile {
    Profile {
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
    }
}

fn request(
    dictionary: ProfilesDictionary,
    resource: Vec<KeyValue>,
    profile: Profile,
) -> ExportProfilesServiceRequest {
    ExportProfilesServiceRequest {
        dictionary: Some(dictionary),
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: resource,
                ..Resource::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                profiles: vec![profile],
                ..ScopeProfiles::default()
            }],
            ..ResourceProfiles::default()
        }],
    }
}

/// §0's request, timed `now`.
fn r(now: i64) -> ExportProfilesServiceRequest {
    request(
        dictionary(),
        vec![
            string_kv("service.name", "checkout"),
            string_kv("host.name", "h1"),
        ],
        profile(now as u64 * 1_000_000),
    )
}

fn inputs(req: &ExportProfilesServiceRequest) -> Vec<ProfileInput> {
    to_inputs(req, false).unwrap_or_else(|r| panic!("converted: {}", r.message))
}

fn parse(req: &ExportProfilesServiceRequest, now: i64) -> ParsedProfileLanding {
    parse_profiles(inputs(req), now).unwrap_or_else(|r| panic!("parsed: {}", r.message))
}

fn names(s: &LandingStack) -> Vec<&str> {
    s.names.iter().map(|n| &**n).collect()
}

fn labels_of(json: &str) -> BTreeMap<String, String> {
    serde_json::from_str(json).expect("a labels object")
}

/// The value of the sample whose stack is `stack`, with its span and trace.
fn sample_on(p: &ParsedProfileLanding, stack: &[&str]) -> (i64, u64, [u8; 16]) {
    let id = p
        .stacks
        .iter()
        .find(|s| names(s) == stack)
        .unwrap_or_else(|| panic!("a stack {stack:?} in {:?}", p.stacks))
        .stack_id;
    let s = p
        .samples
        .iter()
        .find(|s| s.stack_id == id)
        .unwrap_or_else(|| panic!("a sample on {stack:?}"));
    (s.value, s.span_id, s.trace_id)
}

/// **M1.** §0's request becomes §0's rows.
#[test]
fn m1_the_example_becomes_the_example_rows() {
    let now = now_ms();
    let p = parse(&r(now), now);

    assert_eq!(p.series.len(), 1, "{:?}", p.series);
    let series = &p.series[0];
    assert_eq!(
        &*series.type_id,
        "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
    );
    assert_eq!(&*series.service, "checkout");
    assert_eq!(series.timestamp_ns, now * 1_000_000);
    assert_eq!(
        series.labels,
        r#"{"__name__":"process_cpu","__otel__":"true","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:cpu:nanoseconds:cpu:nanoseconds","__service_name__":"checkout","__type__":"cpu","__unit__":"nanoseconds","host.name":"h1","service.name":"checkout","service_name":"checkout"}"#
    );

    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].profile_id, Some(P));
    assert_eq!(p.entries[0].total, 40_000_000);
    assert_eq!(p.entries[0].duration_ns, 10_000_000_000);

    assert_eq!(p.samples.len(), 2);
    assert_eq!(sample_on(&p, &["main.work"]), (30_000_000, 0, [0; 16]));
    assert_eq!(
        sample_on(&p, &["main.work", "runtime.mallocgc"]),
        (10_000_000, u64::from_le_bytes(SPAN), TRACE)
    );
    assert_eq!(p.stacks.len(), 2);
    assert_eq!(p.locations.len(), 2);
}

/// **M2.** A resource without `service.name` is `unknown_service`, with each
/// `process.executable.name` appended.
#[test]
fn m2_a_resource_without_a_service_is_unknown_service() {
    let now = now_ms();
    let time = now as u64 * 1_000_000;
    let with_exe = request(
        dictionary(),
        vec![string_kv("process.executable.name", "x")],
        profile(time),
    );
    let services = |req: &ExportProfilesServiceRequest| -> Vec<String> {
        parse(req, now)
            .series
            .iter()
            .map(|s| s.service.to_string())
            .collect()
    };
    assert_eq!(services(&with_exe), ["unknown_service:x"]);
    let bare = request(dictionary(), Vec::new(), profile(time));
    assert_eq!(services(&bare), ["unknown_service"]);
}

/// A dictionary whose attribute table holds `service.name` = `a` (1) and
/// `b` (2).
fn service_dictionary() -> ProfilesDictionary {
    let mut d = dictionary();
    d.string_table.push("service.name".to_owned()); // 7
    d.attribute_table
        .push(attr(7, Value::StringValue("a".to_owned())));
    d.attribute_table
        .push(attr(7, Value::StringValue("b".to_owned())));
    d
}

/// **M3.** Samples are grouped by their own `service.name`, in first-seen
/// order, the same on every conversion.
#[test]
fn m3_samples_group_by_their_service_in_first_seen_order() {
    let now = now_ms();
    let mut prof = profile(now as u64 * 1_000_000);
    prof.samples = [1, 2, 1]
        .iter()
        .map(|&a| Sample {
            stack_index: 1,
            values: vec![1],
            attribute_indices: vec![a],
            ..Sample::default()
        })
        .collect();
    let req = request(
        service_dictionary(),
        vec![string_kv("service.name", "checkout")],
        prof,
    );
    let first = inputs(&req);
    let services: Vec<&str> = first
        .iter()
        .map(|i| {
            i.labels
                .iter()
                .find(|(k, _)| k == "service_name")
                .map(|(_, v)| v.as_str())
                .expect("a service_name label")
        })
        .collect();
    assert_eq!(services, ["a", "b"]);
    assert_eq!(first, inputs(&req), "the same request converts alike");

    let p = parse_profiles(first, now).expect("parsed");
    let series: Vec<&str> = p.series.iter().map(|s| &*s.service).collect();
    assert_eq!(series, ["a", "b"]);
}

/// **M4.** A sample's link becomes its span and trace id; no link leaves
/// both zero.
#[test]
fn m4_a_link_becomes_the_span_and_trace_id() {
    let now = now_ms();
    let p = parse(&r(now), now);
    let (_, span, trace) = sample_on(&p, &["main.work", "runtime.mallocgc"]);
    assert_eq!(span.to_le_bytes(), SPAN);
    assert_eq!(trace, TRACE);

    let mut req = r(now);
    req.resource_profiles[0].scope_profiles[0].profiles[0].samples[1].link_index = 0;
    let p = parse(&req, now);
    let (_, span, trace) = sample_on(&p, &["main.work", "runtime.mallocgc"]);
    assert_eq!((span, trace), (0, [0; 16]));
}

/// A request over `strings` whose one profile has the sample type
/// `(1, 2)`, the period type `period_type` and the period `period`, and
/// `samples`, each on stack 1 or 2.
fn typed(
    strings: &[&str],
    period_type: Option<(i32, i32)>,
    period: i64,
    samples: Vec<Sample>,
    now: i64,
) -> ExportProfilesServiceRequest {
    let mut d = dictionary();
    d.string_table = st(strings);
    d.function_table[1].name_strindex = 0;
    d.function_table[2].name_strindex = 0;
    let n = d.string_table.len() as i32;
    d.string_table.push("f1".to_owned());
    d.string_table.push("f2".to_owned());
    d.function_table[1].name_strindex = n;
    d.function_table[2].name_strindex = n + 1;
    let prof = Profile {
        sample_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        period_type: period_type.map(|(t, u)| ValueType {
            type_strindex: t,
            unit_strindex: u,
        }),
        period,
        time_unix_nano: now as u64 * 1_000_000,
        samples,
        ..Profile::default()
    };
    request(d, vec![string_kv("service.name", "svc")], prof)
}

fn sample(stack: i32, values: Vec<i64>, timestamps: usize) -> Sample {
    Sample {
        stack_index: stack,
        values,
        timestamps_unix_nano: vec![1; timestamps],
        ..Sample::default()
    }
}

/// **M5.** `samples:count` with a `cpu:nanoseconds` period is CPU time: the
/// count times the period.
#[test]
fn m5_samples_count_with_a_cpu_period_is_cpu_time() {
    let now = now_ms();
    let req = typed(
        &["", "samples", "count", "cpu", "nanoseconds"],
        Some((3, 4)),
        10_000_000,
        vec![sample(1, vec![3], 0)],
        now,
    );
    let p = parse(&req, now);
    assert_eq!(p.samples.len(), 1);
    assert_eq!(p.samples[0].value, 30_000_000);
    assert_eq!(
        &*p.series[0].type_id,
        "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
    );
    assert_eq!(labels_of(&p.series[0].labels)["__name__"], "process_cpu");
}

/// **M6.** `off_cpu:nanoseconds` with no period sums a sample's values; a
/// sample with only timestamps counts them.
#[test]
fn m6_off_cpu_sums_values_and_counts_timestamps() {
    let now = now_ms();
    let req = typed(
        &["", "off_cpu", "nanoseconds"],
        None,
        0,
        vec![sample(1, vec![4, 5], 0), sample(2, Vec::new(), 3)],
        now,
    );
    let p = parse(&req, now);
    assert_eq!(sample_on(&p, &["f1"]).0, 9);
    assert_eq!(sample_on(&p, &["f1", "f2"]).0, 3);
    assert_eq!(&*p.series[0].type_id, "off_cpu:off_cpu:nanoseconds::");
    assert_eq!(labels_of(&p.series[0].labels)["__name__"], "off_cpu");
}

/// **M7.** A sample's value is the sum of its values, one per timestamp;
/// a count of values unlike the count of timestamps is refused.
#[test]
fn m7_values_per_timestamp_sum_and_a_length_mismatch_is_refused() {
    let now = now_ms();
    let strings = &["", "alloc_space", "bytes", "space"];
    let req = typed(
        strings,
        Some((3, 2)),
        524_288,
        vec![sample(1, vec![2, 2, 3, 3], 4)],
        now,
    );
    let p = parse(&req, now);
    assert_eq!(p.samples.len(), 1);
    assert_eq!(p.samples[0].value, 10);
    assert_eq!(&*p.series[0].type_id, "space:alloc_space:bytes:space:bytes");

    let bad = typed(
        strings,
        Some((3, 2)),
        524_288,
        vec![sample(1, vec![2, 2], 3)],
        now,
    );
    let refusal = to_inputs(&bad, false).expect_err("refused");
    assert_eq!(
        refusal.message,
        "failed to convert otel profile: sample values length mismatch 2 3"
    );
}

/// **M8.** A sample's attributes become labels, with the resource's and
/// the scope's; an empty value adds nothing, and the resource wins a key
/// both name.
#[test]
fn m8_sample_resource_and_scope_attributes_become_labels() {
    let now = now_ms();
    let mut d = dictionary();
    let base = d.string_table.len() as i32;
    for s in ["k", "v", "n", "arr", "e"] {
        d.string_table.push(s.to_owned());
    }
    let first = d.attribute_table.len() as i32;
    d.attribute_table
        .push(attr(base, Value::StringValueStrindex(base + 1)));
    d.attribute_table.push(attr(base + 2, Value::IntValue(7)));
    d.attribute_table.push(attr(
        base + 3,
        Value::ArrayValue(ArrayValue {
            values: vec![AnyValue {
                value: Some(Value::StringValue("x".to_owned())),
            }],
        }),
    ));
    d.attribute_table
        .push(attr(base + 4, Value::StringValue(String::new())));
    let mut prof = profile(now as u64 * 1_000_000);
    prof.samples.truncate(1);
    prof.samples[0].attribute_indices = (first..first + 4).collect();
    let mut req = request(
        d,
        vec![
            string_kv("service.name", "checkout"),
            string_kv("host.name", "h"),
        ],
        prof,
    );
    req.resource_profiles[0].scope_profiles[0].scope = Some(InstrumentationScope {
        attributes: vec![string_kv("host.name", "o")],
        ..InstrumentationScope::default()
    });
    let p = parse(&req, now);
    assert_eq!(p.series.len(), 1);
    let labels = labels_of(&p.series[0].labels);
    for (k, v) in [("k", "v"), ("n", "7"), ("arr", "x"), ("host.name", "h")] {
        assert_eq!(
            labels.get(k).map(String::as_str),
            Some(v),
            "{k} in {labels:?}"
        );
    }
    assert!(!labels.contains_key("e"), "{labels:?}");
}

/// **M9.** The resource's `telemetry.sdk.language` decides the Go fix-ups
/// before the symbols do.
#[test]
fn m9_the_sdk_language_decides_the_go_fix_ups() {
    let now = now_ms();
    let build = |language: Option<&str>, file: &str| {
        let mut d = dictionary();
        let name = d.string_table.len() as i32;
        d.string_table.push("pkg.F[go.shape.int]".to_owned());
        d.function_table[1].name_strindex = name;
        if !file.is_empty() {
            d.string_table.push(file.to_owned());
            d.function_table[1].filename_strindex = name + 1;
        }
        let mut prof = profile(now as u64 * 1_000_000);
        prof.samples.truncate(1);
        let mut resource = vec![string_kv("service.name", "svc")];
        if let Some(l) = language {
            resource.push(string_kv("telemetry.sdk.language", l));
        }
        request(d, resource, prof)
    };
    let stack_name = |req: &ExportProfilesServiceRequest| {
        let p = parse(req, now);
        assert_eq!(p.stacks.len(), 1);
        p.stacks[0].names[0].to_string()
    };
    assert_eq!(stack_name(&build(Some("go"), "")), "pkg.F");
    assert_eq!(stack_name(&build(None, "")), "pkg.F[go.shape.int]");
    assert_eq!(
        stack_name(&build(Some("java"), "x.go")),
        "pkg.F[go.shape.int]"
    );
}

/// **M10** (issue #828): each mapping's flags are what its locations in a
/// sample's stack show, as the reference converts them: symbolised lines
/// with files and `line > 0` set `has_functions`, `has_filenames` and
/// `has_line_numbers` (never `has_inline_frames`, which a first line
/// clears); an address-only location clears all four; a `line` of 0 clears
/// `has_line_numbers`. A `has_functions` mapping then loses its bounds and
/// addresses at ingest, as a pprof push's does.
#[test]
fn m10_a_mapping_carries_its_locations_flags() {
    let dict = ProfilesDictionary {
        string_table: st(&[
            "",
            "samples",
            "count",
            "cpu",
            "nanoseconds",
            "main",
            "a",
            "main.go",
            "app",
            "libx.so",
            "lib3",
        ]),
        mapping_table: vec![
            Mapping::default(),
            Mapping {
                memory_start: 0x40_0000,
                memory_limit: 0x80_0000,
                filename_strindex: 8,
                ..Mapping::default()
            },
            Mapping {
                memory_start: 0x7f_0000,
                memory_limit: 0x7f_ffff,
                filename_strindex: 9,
                ..Mapping::default()
            },
            Mapping {
                memory_start: 0x90_0000,
                memory_limit: 0x9f_ffff,
                filename_strindex: 10,
                ..Mapping::default()
            },
        ],
        function_table: vec![
            Function::default(),
            Function {
                name_strindex: 5,
                filename_strindex: 7,
                ..Function::default()
            },
            Function {
                name_strindex: 6,
                filename_strindex: 7,
                ..Function::default()
            },
        ],
        location_table: vec![
            Location::default(),
            Location {
                mapping_index: 1,
                address: 0x40_1000,
                lines: vec![Line {
                    function_index: 1,
                    line: 10,
                    ..Line::default()
                }],
                ..Location::default()
            },
            Location {
                mapping_index: 1,
                address: 0x40_2000,
                lines: vec![Line {
                    function_index: 2,
                    line: 20,
                    ..Line::default()
                }],
                ..Location::default()
            },
            Location {
                mapping_index: 2,
                address: 0x7f_0100,
                ..Location::default()
            },
            Location {
                mapping_index: 3,
                address: 0x90_0100,
                lines: vec![Line {
                    function_index: 1,
                    line: 0,
                    ..Line::default()
                }],
                ..Location::default()
            },
        ],
        stack_table: vec![
            Stack::default(),
            Stack {
                location_indices: vec![2, 1],
            },
            Stack {
                location_indices: vec![3],
            },
            Stack {
                location_indices: vec![4],
            },
        ],
        link_table: vec![Link::default()],
        attribute_table: vec![KeyValueAndUnit::default()],
    };
    let profile = Profile {
        samples: (1..=3)
            .map(|stack_index| Sample {
                stack_index,
                values: vec![1],
                ..Sample::default()
            })
            .collect(),
        ..profile(now_ms() as u64 * 1_000_000)
    };
    let req = request(dict, vec![string_kv("service.name", "otlp-svc")], profile);
    let ins = inputs(&req);
    let p = &ins[0].profile;
    let flags: BTreeMap<&str, (bool, bool, bool, bool)> = p
        .mapping
        .iter()
        .map(|m| {
            (
                p.string_table[m.filename as usize].as_str(),
                (
                    m.has_functions,
                    m.has_filenames,
                    m.has_line_numbers,
                    m.has_inline_frames,
                ),
            )
        })
        .collect();
    assert_eq!(
        flags,
        BTreeMap::from([
            ("app", (true, true, true, false)),
            ("libx.so", (false, false, false, false)),
            ("lib3", (true, true, false, false)),
        ])
    );

    let rows = parse(&req, now_ms());
    let stored: BTreeMap<&str, Vec<(u8, u64, u64)>> =
        rows.locations.iter().fold(BTreeMap::new(), |mut m, l| {
            m.entry(&*l.mapping_file).or_default().push((
                l.mapping_flags,
                l.address,
                l.mapping_start,
            ));
            m
        });
    assert_eq!(
        stored,
        BTreeMap::from([
            ("app", vec![(7, 0, 0), (7, 0, 0)]),
            ("libx.so", vec![(0, 0x7f_0100, 0x7f_0000)]),
            ("lib3", vec![(3, 0, 0)]),
        ])
    );
}
