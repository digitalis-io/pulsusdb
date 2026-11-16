//! A18: every field of every `google_v1` message encodes under the tag and
//! wire type `proto/google/v1/profile.proto` gives it. Each value sets every
//! field to a non-default value, so a field with a mistyped tag, a wrong
//! scalar type or a missing `repeated` changes the bytes. The expected bytes
//! are built field by field with `prost::encoding`, in tag order, with the
//! tag and type read off the proto file.
//!
//! W1 (issue #821): the same for every `types_v1` and `querier_v1` message
//! the query API uses, against `proto/types/v1/types.proto` and
//! `proto/querier/v1/querier.proto`. Issue #823 adds the merge's messages.

use prost::Message;
use prost::encoding;
use pulsus_profiles_wire::google_v1::{
    Function, Label, Line, Location, Mapping, Profile, Sample, ValueType,
};
use pulsus_profiles_wire::querier_v1::{
    FlameGraph, Level, ProfileFormat, ProfileTypesRequest, ProfileTypesResponse,
    SelectMergeStacktracesRequest, SelectMergeStacktracesResponse, SeriesRequest, SeriesResponse,
};
use pulsus_profiles_wire::types_v1::{
    GoPgo, LabelNamesRequest, LabelNamesResponse, LabelPair, LabelValuesRequest,
    LabelValuesResponse, Labels, Location as CallSite, ProfileType, StackTraceSelector,
};

fn value_type() -> ValueType {
    ValueType { r#type: 3, unit: 4 }
}

fn value_type_bytes(v: &ValueType) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::int64::encode(1, &v.r#type, &mut b);
    encoding::int64::encode(2, &v.unit, &mut b);
    b
}

fn label() -> Label {
    Label {
        key: 1,
        str: 2,
        num: -3,
        num_unit: 4,
    }
}

fn label_bytes(l: &Label) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::int64::encode(1, &l.key, &mut b);
    encoding::int64::encode(2, &l.str, &mut b);
    encoding::int64::encode(3, &l.num, &mut b);
    encoding::int64::encode(4, &l.num_unit, &mut b);
    b
}

fn sample() -> Sample {
    Sample {
        location_id: vec![1, 2, 300],
        value: vec![5, -6],
        label: vec![label()],
    }
}

fn sample_bytes(s: &Sample) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint64::encode_packed(1, &s.location_id, &mut b);
    encoding::int64::encode_packed(2, &s.value, &mut b);
    encoding::message::encode_repeated(3, &s.label, &mut b);
    b
}

fn mapping() -> Mapping {
    Mapping {
        id: 1,
        memory_start: 4096,
        memory_limit: 8192,
        file_offset: 16,
        filename: 5,
        build_id: 6,
        has_functions: true,
        has_filenames: true,
        has_line_numbers: true,
        has_inline_frames: true,
    }
}

fn mapping_bytes(m: &Mapping) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint64::encode(1, &m.id, &mut b);
    encoding::uint64::encode(2, &m.memory_start, &mut b);
    encoding::uint64::encode(3, &m.memory_limit, &mut b);
    encoding::uint64::encode(4, &m.file_offset, &mut b);
    encoding::int64::encode(5, &m.filename, &mut b);
    encoding::int64::encode(6, &m.build_id, &mut b);
    encoding::bool::encode(7, &m.has_functions, &mut b);
    encoding::bool::encode(8, &m.has_filenames, &mut b);
    encoding::bool::encode(9, &m.has_line_numbers, &mut b);
    encoding::bool::encode(10, &m.has_inline_frames, &mut b);
    b
}

fn line() -> Line {
    Line {
        function_id: 7,
        line: 42,
    }
}

fn line_bytes(l: &Line) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint64::encode(1, &l.function_id, &mut b);
    encoding::int64::encode(2, &l.line, &mut b);
    b
}

fn location() -> Location {
    Location {
        id: 2,
        mapping_id: 1,
        address: 77,
        line: vec![line()],
        is_folded: true,
    }
}

fn location_bytes(l: &Location) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint64::encode(1, &l.id, &mut b);
    encoding::uint64::encode(2, &l.mapping_id, &mut b);
    encoding::uint64::encode(3, &l.address, &mut b);
    encoding::message::encode_repeated(4, &l.line, &mut b);
    encoding::bool::encode(5, &l.is_folded, &mut b);
    b
}

fn function() -> Function {
    Function {
        id: 7,
        name: 8,
        system_name: 9,
        filename: 10,
        start_line: 11,
    }
}

fn function_bytes(f: &Function) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint64::encode(1, &f.id, &mut b);
    encoding::int64::encode(2, &f.name, &mut b);
    encoding::int64::encode(3, &f.system_name, &mut b);
    encoding::int64::encode(4, &f.filename, &mut b);
    encoding::int64::encode(5, &f.start_line, &mut b);
    b
}

fn profile() -> Profile {
    Profile {
        sample_type: vec![value_type()],
        sample: vec![sample()],
        mapping: vec![mapping()],
        location: vec![location()],
        function: vec![function()],
        string_table: vec![String::new(), "cpu".to_string()],
        drop_frames: 12,
        keep_frames: 13,
        time_nanos: 1_800_000_000_000_000_000,
        duration_nanos: 10_000_000_000,
        period_type: Some(ValueType { r#type: 1, unit: 2 }),
        period: 100,
        comment: vec![1, 2],
        default_sample_type: 1,
    }
}

fn profile_bytes(p: &Profile) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::message::encode_repeated(1, &p.sample_type, &mut b);
    encoding::message::encode_repeated(2, &p.sample, &mut b);
    encoding::message::encode_repeated(3, &p.mapping, &mut b);
    encoding::message::encode_repeated(4, &p.location, &mut b);
    encoding::message::encode_repeated(5, &p.function, &mut b);
    encoding::string::encode_repeated(6, &p.string_table, &mut b);
    encoding::int64::encode(7, &p.drop_frames, &mut b);
    encoding::int64::encode(8, &p.keep_frames, &mut b);
    encoding::int64::encode(9, &p.time_nanos, &mut b);
    encoding::int64::encode(10, &p.duration_nanos, &mut b);
    encoding::message::encode(11, p.period_type.as_ref().expect("set"), &mut b);
    encoding::int64::encode(12, &p.period, &mut b);
    encoding::int64::encode_packed(13, &p.comment, &mut b);
    encoding::int64::encode(14, &p.default_sample_type, &mut b);
    b
}

fn check<M: Message>(name: &str, value: &M, expected: Vec<u8>) {
    assert_eq!(
        value.encode_to_vec(),
        expected,
        "{name}: encoded bytes differ from the proto's tags"
    );
}

#[test]
fn every_field_encodes_under_the_proto_tag() {
    let vt = value_type();
    check("ValueType", &vt, value_type_bytes(&vt));
    let l = label();
    check("Label", &l, label_bytes(&l));
    let s = sample();
    check("Sample", &s, sample_bytes(&s));
    let m = mapping();
    check("Mapping", &m, mapping_bytes(&m));
    let li = line();
    check("Line", &li, line_bytes(&li));
    let lo = location();
    check("Location", &lo, location_bytes(&lo));
    let f = function();
    check("Function", &f, function_bytes(&f));
    let p = profile();
    check("Profile", &p, profile_bytes(&p));
}

fn pair(name: &str, value: &str) -> LabelPair {
    LabelPair {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn pair_bytes(p: &LabelPair) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::string::encode(1, &p.name, &mut b);
    encoding::string::encode(2, &p.value, &mut b);
    b
}

fn profile_type() -> ProfileType {
    ProfileType {
        id: "a:b:c:d:e".to_string(),
        name: "a".to_string(),
        sample_type: "b".to_string(),
        sample_unit: "c".to_string(),
        period_type: "d".to_string(),
        period_unit: "e".to_string(),
    }
}

fn profile_type_bytes(t: &ProfileType) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::string::encode(1, &t.id, &mut b);
    encoding::string::encode(2, &t.name, &mut b);
    encoding::string::encode(4, &t.sample_type, &mut b);
    encoding::string::encode(5, &t.sample_unit, &mut b);
    encoding::string::encode(6, &t.period_type, &mut b);
    encoding::string::encode(7, &t.period_unit, &mut b);
    b
}

fn labels() -> Labels {
    Labels {
        labels: vec![pair("a", "1"), pair("b", "2")],
    }
}

fn labels_bytes(l: &Labels) -> Vec<u8> {
    let mut b = Vec::new();
    for p in &l.labels {
        nested(1, &pair_bytes(p), &mut b);
    }
    b
}

/// One length-delimited field holding `inner`, written without the nested
/// type's own encoder.
fn nested(tag: u32, inner: &[u8], b: &mut Vec<u8>) {
    encoding::encode_key(tag, encoding::WireType::LengthDelimited, b);
    encoding::encode_varint(inner.len() as u64, b);
    b.extend_from_slice(inner);
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn every_query_message_field_encodes_under_the_proto_tag() {
    let p = pair("n", "v");
    assert_eq!(p.encode_to_vec(), pair_bytes(&p), "LabelPair");
    let t = profile_type();
    assert_eq!(t.encode_to_vec(), profile_type_bytes(&t), "ProfileType");
    let l = labels();
    assert_eq!(l.encode_to_vec(), labels_bytes(&l), "Labels");

    let m = LabelNamesRequest {
        matchers: strings(&["{a=\"1\"}", "{b=\"2\"}"]),
        start: 1000,
        end: 2000,
    };
    let mut b = Vec::new();
    encoding::string::encode_repeated(1, &m.matchers, &mut b);
    encoding::int64::encode(2, &m.start, &mut b);
    encoding::int64::encode(3, &m.end, &mut b);
    assert_eq!(m.encode_to_vec(), b, "LabelNamesRequest");

    let m = LabelNamesResponse {
        names: strings(&["a", "b"]),
    };
    let mut b = Vec::new();
    encoding::string::encode_repeated(1, &m.names, &mut b);
    assert_eq!(m.encode_to_vec(), b, "LabelNamesResponse");

    let m = LabelValuesRequest {
        name: "env".to_string(),
        matchers: strings(&["{a=\"1\"}"]),
        start: 1000,
        end: 2000,
    };
    let mut b = Vec::new();
    encoding::string::encode(1, &m.name, &mut b);
    encoding::string::encode_repeated(2, &m.matchers, &mut b);
    encoding::int64::encode(3, &m.start, &mut b);
    encoding::int64::encode(4, &m.end, &mut b);
    assert_eq!(m.encode_to_vec(), b, "LabelValuesRequest");

    let m = LabelValuesResponse {
        names: strings(&["dev", "prod"]),
    };
    let mut b = Vec::new();
    encoding::string::encode_repeated(1, &m.names, &mut b);
    assert_eq!(m.encode_to_vec(), b, "LabelValuesResponse");

    let m = ProfileTypesRequest {
        start: 1000,
        end: 2000,
    };
    let mut b = Vec::new();
    encoding::int64::encode(1, &m.start, &mut b);
    encoding::int64::encode(2, &m.end, &mut b);
    assert_eq!(m.encode_to_vec(), b, "ProfileTypesRequest");

    let m = ProfileTypesResponse {
        profile_types: vec![profile_type(), profile_type()],
    };
    let mut b = Vec::new();
    for t in &m.profile_types {
        nested(1, &profile_type_bytes(t), &mut b);
    }
    assert_eq!(m.encode_to_vec(), b, "ProfileTypesResponse");

    let m = SeriesRequest {
        matchers: strings(&["{a=\"1\"}"]),
        label_names: strings(&["a", "b"]),
        start: 1000,
        end: 2000,
    };
    let mut b = Vec::new();
    encoding::string::encode_repeated(1, &m.matchers, &mut b);
    encoding::string::encode_repeated(2, &m.label_names, &mut b);
    encoding::int64::encode(3, &m.start, &mut b);
    encoding::int64::encode(4, &m.end, &mut b);
    assert_eq!(m.encode_to_vec(), b, "SeriesRequest");

    let m = SeriesResponse {
        labels_set: vec![labels(), Labels::default()],
    };
    let mut b = Vec::new();
    for l in &m.labels_set {
        nested(2, &labels_bytes(l), &mut b);
    }
    assert_eq!(m.encode_to_vec(), b, "SeriesResponse");
}

fn call_site(name: &str) -> CallSite {
    CallSite {
        name: name.to_string(),
    }
}

fn call_site_bytes(l: &CallSite) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::string::encode(1, &l.name, &mut b);
    b
}

fn go_pgo() -> GoPgo {
    GoPgo {
        keep_locations: 3,
        aggregate_callees: true,
    }
}

fn go_pgo_bytes(g: &GoPgo) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::uint32::encode(1, &g.keep_locations, &mut b);
    encoding::bool::encode(2, &g.aggregate_callees, &mut b);
    b
}

fn selector() -> StackTraceSelector {
    StackTraceSelector {
        call_site: vec![call_site("main"), call_site("a")],
        go_pgo: Some(go_pgo()),
    }
}

fn selector_bytes(s: &StackTraceSelector) -> Vec<u8> {
    let mut b = Vec::new();
    for l in &s.call_site {
        nested(1, &call_site_bytes(l), &mut b);
    }
    nested(2, &go_pgo_bytes(s.go_pgo.as_ref().expect("set")), &mut b);
    b
}

fn level(values: &[i64]) -> Level {
    Level {
        values: values.to_vec(),
    }
}

fn level_bytes(l: &Level) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::int64::encode_packed(1, &l.values, &mut b);
    b
}

fn flame_graph() -> FlameGraph {
    FlameGraph {
        names: strings(&["total", "main"]),
        levels: vec![level(&[0, 32, 0, 0]), level(&[0, 32, -1, 1])],
        total: 32,
        max_self: 17,
    }
}

fn flame_graph_bytes(f: &FlameGraph) -> Vec<u8> {
    let mut b = Vec::new();
    encoding::string::encode_repeated(1, &f.names, &mut b);
    for l in &f.levels {
        nested(2, &level_bytes(l), &mut b);
    }
    encoding::int64::encode(3, &f.total, &mut b);
    encoding::int64::encode(4, &f.max_self, &mut b);
    b
}

/// W1 (issue #823): the merge's messages, every field set.
#[test]
fn every_merge_message_field_encodes_under_the_proto_tag() {
    let l = call_site("main");
    assert_eq!(l.encode_to_vec(), call_site_bytes(&l), "Location");
    let g = go_pgo();
    assert_eq!(g.encode_to_vec(), go_pgo_bytes(&g), "GoPGO");
    let s = selector();
    assert_eq!(s.encode_to_vec(), selector_bytes(&s), "StackTraceSelector");
    let l = level(&[1, -2, 300]);
    assert_eq!(l.encode_to_vec(), level_bytes(&l), "Level");
    let f = flame_graph();
    assert_eq!(f.encode_to_vec(), flame_graph_bytes(&f), "FlameGraph");

    let m = SelectMergeStacktracesResponse {
        flamegraph: Some(flame_graph()),
    };
    let mut b = Vec::new();
    nested(1, &flame_graph_bytes(&flame_graph()), &mut b);
    assert_eq!(m.encode_to_vec(), b, "SelectMergeStacktracesResponse");

    let m = SelectMergeStacktracesRequest {
        profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
        label_selector: "{service_name=\"a\"}".to_string(),
        start: 1000,
        end: 2000,
        max_nodes: Some(4),
        format: ProfileFormat::Tree as i32,
        stack_trace_selector: Some(selector()),
        profile_id_selector: strings(&["7c9e6679-7425-40de-944b-e07fc1f90ae7"]),
        trace_id_selector: strings(&["0102030405060708090a0b0c0d0e0f10"]),
        span_selector: strings(&["0000000000000001", "0000000000000002"]),
    };
    let mut b = Vec::new();
    encoding::string::encode(1, &m.profile_type_id, &mut b);
    encoding::string::encode(2, &m.label_selector, &mut b);
    encoding::int64::encode(3, &m.start, &mut b);
    encoding::int64::encode(4, &m.end, &mut b);
    encoding::int64::encode(5, &4, &mut b);
    encoding::int32::encode(6, &2, &mut b);
    nested(7, &selector_bytes(&selector()), &mut b);
    encoding::string::encode_repeated(8, &m.profile_id_selector, &mut b);
    encoding::string::encode_repeated(10, &m.trace_id_selector, &mut b);
    encoding::string::encode_repeated(11, &m.span_selector, &mut b);
    assert_eq!(m.encode_to_vec(), b, "SelectMergeStacktracesRequest");

    // An explicit `max_nodes` of 0 is present on the wire, as `optional`
    // has it; an absent one is not.
    let zero = SelectMergeStacktracesRequest {
        max_nodes: Some(0),
        ..SelectMergeStacktracesRequest::default()
    };
    let mut b = Vec::new();
    encoding::int64::encode(5, &0, &mut b);
    assert_eq!(zero.encode_to_vec(), b, "max_nodes 0");
    assert!(SelectMergeStacktracesRequest::default()
        .encode_to_vec()
        .is_empty());
}
