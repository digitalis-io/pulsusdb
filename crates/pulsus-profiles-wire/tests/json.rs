//! W2 and W3 (issue #821): the query messages' proto3 JSON mapping. Out:
//! lowerCamelCase names, a field at its default omitted. In: a field by its
//! JSON name or its proto name, an `int64` as a number or a decimal string,
//! an unknown field ignored. W4 and W5 (issue #823): the merge's messages,
//! an enum by its name or its number. W6 and W7 (issue #824): the time
//! series' messages, a `double` written as the reference writes it. W8
//! (issue #827): the diff's messages. W9 and W10 (issue #825): the
//! heatmap's messages. W11 (#823 part B): the flame graph's tree bytes.
//! W11 (issue #826): an exemplar. W12 (issue #828): the pprof `Profile`'s
//! JSON, and `SelectMergeProfile`'s request as the exploration app sends it.
//! W14 (issue #830): the settings, stats and query analysis messages.

use pulsus_profiles_wire::google_v1::Profile;
use pulsus_profiles_wire::querier_v1::{
    AnalyzeQueryRequest, AnalyzeQueryResponse, DiffRequest, DiffResponse, FlameGraph,
    FlameGraphDiff, Level, ProfileFormat, ProfileTypesRequest, ProfileTypesResponse, QueryScope,
    SelectHeatmapRequest, SelectHeatmapResponse, SelectMergeProfileRequest,
    SelectMergeStacktracesRequest, SelectMergeStacktracesResponse, SelectSeriesRequest,
    SelectSeriesResponse, SeriesRequest, SeriesResponse,
};
use pulsus_profiles_wire::settings_v1::{
    GetSettingsResponse, SetSettingsRequest, SetSettingsResponse, Setting,
};
use pulsus_profiles_wire::types_v1::{
    Exemplar, GetProfileStatsResponse, GoPgo, HeatmapSeries, HeatmapSlot, LabelPair, Labels,
    Location, Point, ProfileType, Series, StackTraceSelector,
};

fn split(id: &str) -> ProfileType {
    let parts: Vec<&str> = id.split(':').collect();
    ProfileType {
        id: id.to_string(),
        name: parts[0].to_string(),
        sample_type: parts[1].to_string(),
        sample_unit: parts[2].to_string(),
        period_type: parts[3].to_string(),
        period_unit: parts[4].to_string(),
    }
}

/// W2: the response messages as the reference writes them.
#[test]
fn w2_responses_are_lower_camel_case_with_defaults_omitted() {
    let types = ProfileTypesResponse {
        profile_types: vec![split("a:b:c:d:e")],
    };
    assert_eq!(
        serde_json::to_string(&types).expect("encode"),
        r#"{"profileTypes":[{"ID":"a:b:c:d:e","name":"a","sampleType":"b","sampleUnit":"c","periodType":"d","periodUnit":"e"}]}"#
    );
    assert_eq!(
        serde_json::to_string(&ProfileTypesResponse::default()).expect("encode"),
        "{}"
    );
    let series = SeriesResponse {
        labels_set: vec![
            Labels::default(),
            Labels {
                labels: vec![LabelPair {
                    name: "a".to_string(),
                    value: "1".to_string(),
                }],
            },
        ],
    };
    assert_eq!(
        serde_json::to_string(&series).expect("encode"),
        r#"{"labelsSet":[{},{"labels":[{"name":"a","value":"1"}]}]}"#
    );
}

/// W3: the request messages read either name, either `int64` form, and
/// ignore what they do not know.
#[test]
fn w3_requests_read_either_name_and_either_int64_form() {
    // Each decode is compared as a `Result`, so a refusal fails the
    // assertion rather than stopping before it.
    let r = serde_json::from_str::<ProfileTypesRequest>(r#"{"start":"1000","end":2000}"#)
        .map(|r| (r.start, r.end))
        .map_err(|e| e.to_string());
    assert_eq!(r, Ok((1000, 2000)));
    for body in [r#"{"label_names":["a"]}"#, r#"{"labelNames":["a"]}"#] {
        let r = serde_json::from_str::<SeriesRequest>(body)
            .map(|r| r.label_names)
            .map_err(|e| e.to_string());
        assert_eq!(r, Ok(vec!["a".to_string()]), "{body}");
    }
    let r = serde_json::from_str::<SeriesRequest>(r#"{"x":1}"#).map_err(|e| e.to_string());
    assert_eq!(r, Ok(SeriesRequest::default()));
    assert!(serde_json::from_str::<ProfileTypesRequest>(r#"{"start":"x"}"#).is_err());
}

fn level(values: &[i64]) -> Level {
    Level {
        values: values.to_vec(),
    }
}

/// W4: the flame graph as the reference writes it — plan §3.1's first row,
/// and the empty flame graph with its zero total and max self omitted.
#[test]
fn w4_the_flame_graph_is_written_as_the_reference_writes_it() {
    let row1 = SelectMergeStacktracesResponse {
        flamegraph: Some(FlameGraph {
            names: ["total", "main", "other", "e", "f", "d", "a", "c", "b"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            levels: vec![
                level(&[0, 32, 0, 0]),
                level(&[0, 32, 0, 1]),
                level(&[0, 24, 2, 6, 0, 3, 3, 5, 0, 1, 0, 3, 0, 4, 4, 2]),
                level(&[2, 17, 17, 8, 0, 5, 5, 7, 3, 1, 1, 4]),
            ],
            total: 32,
            max_self: 17,
        }),
        tree: Vec::new(),
        dot: String::new(),
        pprof: None,
    };
    assert_eq!(
        serde_json::to_string(&row1).expect("encode"),
        r#"{"flamegraph":{"names":["total","main","other","e","f","d","a","c","b"],"levels":[{"values":["0","32","0","0"]},{"values":["0","32","0","1"]},{"values":["0","24","2","6","0","3","3","5","0","1","0","3","0","4","4","2"]},{"values":["2","17","17","8","0","5","5","7","3","1","1","4"]}],"total":"32","maxSelf":"17"}}"#
    );
    let empty = SelectMergeStacktracesResponse {
        flamegraph: Some(FlameGraph {
            names: vec!["total".to_string()],
            levels: vec![level(&[0, 0, 0, 0])],
            total: 0,
            max_self: 0,
        }),
        tree: Vec::new(),
        dot: String::new(),
        pprof: None,
    };
    assert_eq!(
        serde_json::to_string(&empty).expect("encode"),
        r#"{"flamegraph":{"names":["total"],"levels":[{"values":["0","0","0","0"]}]}}"#
    );
}

/// W11 (#823 part B): the tree bytes beside the flame graph, field 2, as
/// standard padded base64 in JSON; no bytes, no key.
#[test]
fn w11_the_tree_bytes_are_written_as_the_reference_writes_them() {
    use prost::Message;
    let graph = FlameGraph {
        names: vec!["total".to_string()],
        levels: vec![level(&[0, 0, 0, 0])],
        total: 0,
        max_self: 0,
    };
    let with = SelectMergeStacktracesResponse {
        flamegraph: Some(graph.clone()),
        tree: vec![0, 0, 1],
        dot: String::new(),
        pprof: None,
    };
    let text = serde_json::to_string(&with).expect("encode");
    assert_eq!(
        text,
        r#"{"flamegraph":{"names":["total"],"levels":[{"values":["0","0","0","0"]}]},"tree":"AAAB"}"#
    );
    assert_eq!(
        serde_json::from_str::<SelectMergeStacktracesResponse>(&text).expect("decode"),
        with
    );
    let without = SelectMergeStacktracesResponse {
        flamegraph: Some(graph.clone()),
        tree: Vec::new(),
        dot: String::new(),
        pprof: None,
    };
    assert!(
        !serde_json::to_string(&without)
            .expect("encode")
            .contains("tree"),
        "an empty tree is written"
    );
    let graph_bytes = SelectMergeStacktracesResponse {
        flamegraph: Some(graph),
        tree: Vec::new(),
        dot: String::new(),
        pprof: None,
    }
    .encode_to_vec();
    let mut want = graph_bytes.clone();
    want.extend_from_slice(&[0x12, 3, 0, 0, 1]);
    assert_eq!(with.encode_to_vec(), want, "tree is field 2");
}

/// Issue #829 part B: the DOT text is read and written as `dot`; an empty
/// one is not written.
#[test]
fn the_dot_text_is_read_and_written_as_dot() {
    let r: SelectMergeStacktracesResponse = serde_json::from_str(r#"{"dot":"x"}"#).expect("decode");
    assert_eq!(r.dot, "x");
    assert_eq!(serde_json::to_string(&r).expect("encode"), r#"{"dot":"x"}"#);
    assert_eq!(
        serde_json::to_string(&SelectMergeStacktracesResponse::default()).expect("encode"),
        "{}",
        "an empty dot is written"
    );
}

/// W5: the request reads either name, `maxNodes` as a number or a string,
/// `format` as its name or its number, and ignores `async`.
#[test]
fn w5_the_merge_request_reads_either_form() {
    let expected = SelectMergeStacktracesRequest {
        profile_type_id: "a:b:c:d:e".to_string(),
        label_selector: "{x=\"1\"}".to_string(),
        start: 1000,
        end: 2000,
        max_nodes: Some(4),
        format: ProfileFormat::Flamegraph as i32,
        stack_trace_selector: Some(StackTraceSelector {
            call_site: vec![Location {
                name: "main".to_string(),
            }],
            go_pgo: Some(GoPgo {
                keep_locations: 2,
                aggregate_callees: true,
            }),
        }),
        profile_id_selector: vec!["p".to_string()],
        trace_id_selector: vec!["t".to_string()],
        span_selector: vec!["s".to_string()],
    };
    for body in [
        r#"{"profileTypeID":"a:b:c:d:e","labelSelector":"{x=\"1\"}","start":"1000","end":"2000","maxNodes":4,"format":"PROFILE_FORMAT_FLAMEGRAPH","stackTraceSelector":{"callSite":[{"name":"main"}],"goPgo":{"keepLocations":2,"aggregateCallees":true}},"profileIdSelector":["p"],"traceIdSelector":["t"],"spanSelector":["s"]}"#,
        r#"{"profile_typeID":"a:b:c:d:e","label_selector":"{x=\"1\"}","start":1000,"end":2000,"max_nodes":"4","format":1,"stack_trace_selector":{"call_site":[{"name":"main"}],"go_pgo":{"keep_locations":2,"aggregate_callees":true}},"profile_id_selector":["p"],"trace_id_selector":["t"],"span_selector":["s"],"async":{"x":1}}"#,
    ] {
        let r =
            serde_json::from_str::<SelectMergeStacktracesRequest>(body).map_err(|e| e.to_string());
        assert_eq!(r, Ok(expected.clone()), "{body}");
    }
    // Absent, `maxNodes` is `None`; `format` is unspecified.
    let r = serde_json::from_str::<SelectMergeStacktracesRequest>("{}")
        .map(|r| (r.max_nodes, r.format))
        .map_err(|e| e.to_string());
    assert_eq!(r, Ok((None, 0)));
    for (name, n) in [
        ("PROFILE_FORMAT_UNSPECIFIED", 0),
        ("PROFILE_FORMAT_TREE", 2),
        ("PROFILE_FORMAT_DOT", 3),
        ("PROFILE_FORMAT_PPROF", 4),
    ] {
        let r = serde_json::from_str::<SelectMergeStacktracesRequest>(&format!(
            r#"{{"format":"{name}"}}"#
        ))
        .map(|r| r.format)
        .map_err(|e| e.to_string());
        assert_eq!(r, Ok(n), "{name}");
    }
    assert!(serde_json::from_str::<SelectMergeStacktracesRequest>(r#"{"format":"NOPE"}"#).is_err());
}

/// W6 (issue #824): the request reads a `double` as a number or a string,
/// an enum by its name or its number, and an `optional int64` as a string.
#[test]
fn w6_the_series_request_reads_either_form() {
    let expected = SelectSeriesRequest {
        profile_type_id: "a:b:c:d:e".to_string(),
        label_selector: "{}".to_string(),
        start: 1000,
        end: 2000,
        group_by: vec!["service_name".to_string()],
        step: 15.0,
        aggregation: Some(1),
        limit: Some(1),
        exemplar_type: 1,
        ..SelectSeriesRequest::default()
    };
    for body in [
        r#"{"profileTypeID":"a:b:c:d:e","labelSelector":"{}","start":"1000","end":"2000","groupBy":["service_name"],"step":15,"aggregation":"TIME_SERIES_AGGREGATION_TYPE_AVERAGE","limit":"1","exemplarType":"EXEMPLAR_TYPE_NONE"}"#,
        r#"{"profile_typeID":"a:b:c:d:e","label_selector":"{}","start":1000,"end":2000,"group_by":["service_name"],"step":"15","aggregation":1,"limit":1,"exemplar_type":1}"#,
    ] {
        let got: SelectSeriesRequest = serde_json::from_str(body).expect(body);
        assert_eq!(got, expected, "{body}");
    }
    let got: SelectSeriesRequest =
        serde_json::from_str(r#"{"step":0.5,"exemplarType":"EXEMPLAR_TYPE_INDIVIDUAL"}"#)
            .expect("decode");
    assert_eq!(
        (got.step, got.aggregation, got.exemplar_type),
        (0.5, None, 2)
    );
}

/// W7 (issue #824): an integral point value is written as an integer, as
/// the reference writes it; another as a number; defaults omitted.
#[test]
fn w7_a_point_value_is_written_as_the_reference_writes_it() {
    let point = |value: f64| Point {
        value,
        timestamp: 1_794_819_600_000,
        annotations: Vec::new(),
        exemplars: Vec::new(),
    };
    assert_eq!(
        serde_json::to_string(&point(28_885_319_484.0)).expect("encode"),
        r#"{"value":28885319484,"timestamp":"1794819600000"}"#
    );
    assert_eq!(
        serde_json::to_string(&point(2.5)).expect("encode"),
        r#"{"value":2.5,"timestamp":"1794819600000"}"#
    );
    let response = SelectSeriesResponse {
        series: vec![
            Series {
                labels: vec![LabelPair {
                    name: "pod".to_string(),
                    value: "b".to_string(),
                }],
                points: vec![point(6.0)],
            },
            Series::default(),
        ],
    };
    assert_eq!(
        serde_json::to_string(&response).expect("encode"),
        r#"{"series":[{"labels":[{"name":"pod","value":"b"}],"points":[{"value":6,"timestamp":"1794819600000"}]},{}]}"#
    );
    assert_eq!(
        serde_json::to_string(&SelectSeriesResponse::default()).expect("encode"),
        "{}"
    );
}

/// W8 (issue #827): plan §3.2's g1 body is written as the reference writes
/// it and read back; a request with `left` alone reads `right` as absent.
#[test]
fn w8_the_diff_is_written_as_the_reference_writes_it() {
    const G1: &str = r#"{"flamegraph":{"names":["total","main","b","a"],"levels":[{"values":["0","3","0","0","5","0","0"]},{"values":["0","3","0","0","5","0","1"]},{"values":["0","3","3","0","0","0","3","0","0","0","0","5","5","2"]}],"total":"8","maxSelf":"5","leftTicks":"3","rightTicks":"5"}}"#;
    let g1 = DiffResponse {
        flamegraph: Some(FlameGraphDiff {
            names: ["total", "main", "b", "a"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            levels: vec![
                level(&[0, 3, 0, 0, 5, 0, 0]),
                level(&[0, 3, 0, 0, 5, 0, 1]),
                level(&[0, 3, 3, 0, 0, 0, 3, 0, 0, 0, 0, 5, 5, 2]),
            ],
            total: 8,
            max_self: 5,
            left_ticks: 3,
            right_ticks: 5,
        }),
    };
    assert_eq!(serde_json::to_string(&g1).expect("encode"), G1);
    let r = serde_json::from_str::<DiffResponse>(G1).map_err(|e| e.to_string());
    assert_eq!(r, Ok(g1));
    // The proto names read too.
    let r = serde_json::from_str::<FlameGraphDiff>(r#"{"left_ticks":"3","right_ticks":5}"#)
        .map(|f| (f.left_ticks, f.right_ticks))
        .map_err(|e| e.to_string());
    assert_eq!(r, Ok((3, 5)));

    let r = serde_json::from_str::<DiffRequest>(
        r#"{"left":{"profileTypeID":"a:b:c:d:e","start":"1000","end":"2000"}}"#,
    )
    .map_err(|e| e.to_string());
    assert_eq!(
        r,
        Ok(DiffRequest {
            left: Some(SelectMergeStacktracesRequest {
                profile_type_id: "a:b:c:d:e".to_string(),
                start: 1000,
                end: 2000,
                ..SelectMergeStacktracesRequest::default()
            }),
            right: None,
        })
    );
}

/// W9 (issue #825): the heatmap request reads the query type by its name
/// or its number, a `double` as a number or a string, and an `optional
/// int64` as a string.
#[test]
fn w9_the_heatmap_request_reads_either_form() {
    let expected = SelectHeatmapRequest {
        profile_type_id: "a:b:c:d:e".to_string(),
        label_selector: "{}".to_string(),
        start: 1000,
        end: 2000,
        step: 15.0,
        group_by: vec!["service_name".to_string()],
        query_type: 2,
        exemplar_type: 1,
        limit: Some(1),
    };
    for body in [
        r#"{"profileTypeID":"a:b:c:d:e","labelSelector":"{}","start":"1000","end":"2000","step":15,"groupBy":["service_name"],"queryType":"HEATMAP_QUERY_TYPE_SPAN","exemplarType":"EXEMPLAR_TYPE_NONE","limit":"1"}"#,
        r#"{"profile_typeID":"a:b:c:d:e","label_selector":"{}","start":1000,"end":2000,"step":"15","group_by":["service_name"],"query_type":2,"exemplar_type":1,"limit":1}"#,
    ] {
        let got: SelectHeatmapRequest = serde_json::from_str(body).expect(body);
        assert_eq!(got, expected, "{body}");
    }
    let got: SelectHeatmapRequest =
        serde_json::from_str(r#"{"queryType":"HEATMAP_QUERY_TYPE_INDIVIDUAL"}"#).expect("decode");
    assert_eq!((got.query_type, got.limit), (1, None));
    assert!(
        serde_json::from_str::<SelectHeatmapRequest>(r#"{"queryType":"HEATMAP_QUERY_TYPE_NOPE"}"#)
            .is_err()
    );
    assert_eq!(
        serde_json::to_string(&SelectHeatmapRequest {
            query_type: 1,
            ..SelectHeatmapRequest::default()
        })
        .expect("encode"),
        r#"{"queryType":"HEATMAP_QUERY_TYPE_INDIVIDUAL"}"#
    );
}

/// W10 (issue #825): a slot's `yMin` is written as the reference writes a
/// double, each integral one as an integer; `counts` as numbers; zeros in
/// both kept; defaults omitted.
#[test]
fn w10_a_slot_is_written_as_the_reference_writes_it() {
    let slot = HeatmapSlot {
        timestamp: 1_794_819_600_000,
        y_min: vec![0.0, 2.5],
        counts: vec![1, 0],
        exemplars: Vec::new(),
    };
    let text = r#"{"timestamp":"1794819600000","yMin":[0,2.5],"counts":[1,0]}"#;
    assert_eq!(serde_json::to_string(&slot).expect("encode"), text);
    let back: HeatmapSlot = serde_json::from_str(text).expect("decode");
    assert_eq!(back, slot);
    let response = SelectHeatmapResponse {
        series: vec![
            HeatmapSeries {
                labels: vec![LabelPair {
                    name: "pod".to_string(),
                    value: "b".to_string(),
                }],
                slots: vec![slot],
            },
            HeatmapSeries::default(),
        ],
    };
    assert_eq!(
        serde_json::to_string(&response).expect("encode"),
        format!(
            r#"{{"series":[{{"labels":[{{"name":"pod","value":"b"}}],"slots":[{text}]}},{{}}]}}"#
        )
    );
    assert_eq!(
        serde_json::to_string(&SelectHeatmapResponse::default()).expect("encode"),
        "{}"
    );
}

/// W11 (issue #826): an exemplar is written as the reference writes it,
/// read back by either name, and a value of 0 has no `value`.
#[test]
fn w11_an_exemplar_is_written_as_the_reference_writes_it() {
    let exemplar = Exemplar {
        timestamp: 1_794_819_625_000,
        profile_id: "00000000-0000-0000-0000-0000000000c2".to_string(),
        span_id: "0a00000000000000".to_string(),
        value: 5,
        labels: vec![LabelPair {
            name: "pod".to_string(),
            value: "e".to_string(),
        }],
        trace_id: "00000000000000000000000000000002".to_string(),
    };
    let text = r#"{"timestamp":"1794819625000","profileId":"00000000-0000-0000-0000-0000000000c2","spanId":"0a00000000000000","value":"5","labels":[{"name":"pod","value":"e"}],"traceId":"00000000000000000000000000000002"}"#;
    assert_eq!(serde_json::to_string(&exemplar).expect("encode"), text);
    let back: Exemplar = serde_json::from_str(text).expect("decode");
    assert_eq!(back, exemplar);
    let snake = r#"{"timestamp":"1794819625000","profile_id":"00000000-0000-0000-0000-0000000000c2","span_id":"0a00000000000000","value":"5","labels":[{"name":"pod","value":"e"}],"trace_id":"00000000000000000000000000000002"}"#;
    let back: Exemplar = serde_json::from_str(snake).expect("decode");
    assert_eq!(back, exemplar);
    let zero = Exemplar {
        value: 0,
        ..exemplar
    };
    assert_eq!(
        serde_json::to_string(&zero).expect("encode"),
        r#"{"timestamp":"1794819625000","profileId":"00000000-0000-0000-0000-0000000000c2","spanId":"0a00000000000000","labels":[{"name":"pod","value":"e"}],"traceId":"00000000000000000000000000000002"}"#
    );
}

/// The reference's JSON answer to the exploration app's function-details
/// request on plan §8.1's fixture (#828 plan §4), captured.
const REFERENCE_PROFILE_JSON: &str = r#"{"sampleType":[{"type":"9","unit":"8"}],"sample":[{"locationId":["1","2"],"value":["60000000"]},{"locationId":["3","1","2"],"value":["20000000"]},{"locationId":["4","2"],"value":["40000000"]},{"locationId":["5","4","2"],"value":["200000000"]}],"mapping":[{"id":"1","filename":"2","buildId":"1","hasFunctions":true,"hasFilenames":true,"hasLineNumbers":true,"hasInlineFrames":true}],"location":[{"id":"1","mappingId":"1","line":[{"functionId":"2","line":"21"}]},{"id":"2","mappingId":"1","line":[{"functionId":"1","line":"10"}]},{"id":"3","mappingId":"1","line":[{"functionId":"4","line":"40"}]},{"id":"4","mappingId":"1","line":[{"functionId":"2","line":"20"}]},{"id":"5","mappingId":"1","line":[{"functionId":"3","line":"30"}]}],"function":[{"id":"1","name":"3","systemName":"3","filename":"4","startLine":"1"},{"id":"2","name":"5","systemName":"5","filename":"4","startLine":"2"},{"id":"3","name":"6","systemName":"6","filename":"4","startLine":"3"},{"id":"4","name":"7","systemName":"7","filename":"4","startLine":"4"}],"stringTable":["","bid","app","main","main.go","a","b","c","nanoseconds","cpu"],"timeNanos":"1795085995000000000","periodType":{"type":"9","unit":"8"},"period":"1000000000","defaultSampleType":"9"}"#;

/// **W12** (issue #828): the reference's JSON `Profile` decodes, every
/// field read, and re-encodes to the same JSON; every name of the mapping
/// is the proto3 JSON one.
#[test]
fn w12_the_pprof_profile_is_written_as_the_reference_writes_it() {
    let p: Profile = serde_json::from_str(REFERENCE_PROFILE_JSON).expect("decode");
    assert_eq!(p.sample.len(), 4);
    assert_eq!(p.sample[1].location_id, [3, 1, 2]);
    assert_eq!(p.sample[3].value, [200_000_000]);
    assert_eq!(p.mapping[0].build_id, 1);
    assert!(p.mapping[0].has_inline_frames);
    assert_eq!(p.location[0].line[0].line, 21);
    assert_eq!(p.function[3].start_line, 4);
    assert_eq!(p.time_nanos, 1_795_085_995_000_000_000);
    assert_eq!(p.period, 1_000_000_000);
    assert_eq!(p.default_sample_type, 9);
    assert_eq!(p.period_type.map(|t| (t.r#type, t.unit)), Some((9, 8)));
    let again: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&p).expect("encode")).expect("JSON");
    let want: serde_json::Value = serde_json::from_str(REFERENCE_PROFILE_JSON).expect("JSON");
    assert_eq!(again, want);
    assert_eq!(
        serde_json::to_string(&Profile::default()).expect("encode"),
        "{}"
    );
}

/// **W12** (issue #828): the exploration app's function-details request:
/// the proto names it sends, the call site and `maxNodes` read.
#[test]
fn w12_the_apps_merge_profile_request_is_read() {
    let text = r#"{"profile_typeID":"process_cpu:cpu:nanoseconds:cpu:nanoseconds","label_selector":"{service_name=\"q-pprof\"}","start":1000,"end":2000,"stackTraceSelector":{"call_site":[{"name":"main"},{"name":"a"}]},"maxNodes":500}"#;
    let m: SelectMergeProfileRequest = serde_json::from_str(text).expect("decode");
    assert_eq!(
        m.profile_type_id,
        "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
    );
    assert_eq!(m.label_selector, r#"{service_name="q-pprof"}"#);
    assert_eq!((m.start, m.end, m.max_nodes), (1000, 2000, Some(500)));
    let names: Vec<String> = m
        .stack_trace_selector
        .expect("a selector")
        .call_site
        .into_iter()
        .map(|l| l.name)
        .collect();
    assert_eq!(names, ["main", "a"]);
    let camel = r#"{"profileTypeID":"a:b:c:d:e","labelSelector":"{}","start":"1","end":"2","maxNodes":"3","profileIdSelector":["x"],"traceIdSelector":["y"]}"#;
    let m: SelectMergeProfileRequest = serde_json::from_str(camel).expect("decode");
    assert_eq!(
        (m.profile_type_id.as_str(), m.start, m.end, m.max_nodes),
        ("a:b:c:d:e", 1, 2, Some(3))
    );
    assert_eq!(
        (m.profile_id_selector, m.trace_id_selector),
        (vec!["x".to_string()], vec!["y".to_string()])
    );
}

/// **W14** (issue #830): a setting writes its JSON names, `modifiedAt` as a
/// string, and reads `modifiedAt` as a string or a number; the stats and
/// analysis answers write their names, and an empty one writes `{}`.
#[test]
fn w14_the_settings_stats_and_analysis_json() {
    let s = Setting {
        name: "a".to_string(),
        value: "1".to_string(),
        modified_at: 10,
    };
    assert_eq!(
        serde_json::to_string(&s).expect("encode"),
        r#"{"name":"a","value":"1","modifiedAt":"10"}"#
    );
    for text in [
        r#"{"name":"a","value":"1","modifiedAt":"10"}"#,
        r#"{"name":"a","value":"1","modifiedAt":10}"#,
    ] {
        let got: Setting = serde_json::from_str(text).expect("decode");
        assert_eq!(got, s, "{text}");
    }
    let set: SetSettingsRequest =
        serde_json::from_str(r#"{"setting":{"name":"pluginSettings","value":"v"}}"#)
            .expect("decode");
    assert_eq!(
        set.setting.map(|s| (s.name, s.value, s.modified_at)),
        Some(("pluginSettings".to_string(), "v".to_string(), 0))
    );
    assert_eq!(
        serde_json::to_string(&SetSettingsResponse {
            setting: Some(s.clone())
        })
        .expect("encode"),
        r#"{"setting":{"name":"a","value":"1","modifiedAt":"10"}}"#
    );
    assert_eq!(
        serde_json::to_string(&GetSettingsResponse { settings: vec![s] }).expect("encode"),
        r#"{"settings":[{"name":"a","value":"1","modifiedAt":"10"}]}"#
    );
    assert_eq!(
        serde_json::to_string(&GetSettingsResponse::default()).expect("encode"),
        "{}"
    );
    assert_eq!(
        serde_json::to_string(&GetProfileStatsResponse::default()).expect("encode"),
        "{}"
    );
    assert_eq!(
        serde_json::to_string(&GetProfileStatsResponse {
            data_ingested: true,
            oldest_profile_time: 1,
            newest_profile_time: 2,
        })
        .expect("encode"),
        r#"{"dataIngested":true,"oldestProfileTime":"1","newestProfileTime":"2"}"#
    );
    assert_eq!(
        serde_json::to_string(&AnalyzeQueryResponse::default()).expect("encode"),
        "{}"
    );
    // The reference's answer reads back as two scopes.
    let r: AnalyzeQueryResponse = serde_json::from_str(
        r#"{"queryScopes":[{"componentType":"Short term storage"},{"componentType":"Long term storage","blockCount":"3"}],"queryImpact":{}}"#,
    )
    .expect("decode");
    assert_eq!(
        r.query_scopes,
        [
            QueryScope {
                component_type: "Short term storage".to_string(),
                ..QueryScope::default()
            },
            QueryScope {
                component_type: "Long term storage".to_string(),
                block_count: 3,
                ..QueryScope::default()
            },
        ]
    );
    assert!(r.query_impact.is_some());
    let q: AnalyzeQueryRequest =
        serde_json::from_str(r#"{"start":"1","end":2,"query":"{}"}"#).expect("decode");
    assert_eq!((q.start, q.end, q.query.as_str()), (1, 2, "{}"));
}
