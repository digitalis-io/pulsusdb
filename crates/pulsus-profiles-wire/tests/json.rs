//! W2 and W3 (issue #821): the query messages' proto3 JSON mapping. Out:
//! lowerCamelCase names, a field at its default omitted. In: a field by its
//! JSON name or its proto name, an `int64` as a number or a decimal string,
//! an unknown field ignored. W4 and W5 (issue #823): the merge's messages,
//! an enum by its name or its number. W6 and W7 (issue #824): the time
//! series' messages, a `double` written as the reference writes it. W8 and
//! W9 (issue #825): the heatmap's messages.

use pulsus_profiles_wire::querier_v1::{
    FlameGraph, Level, ProfileFormat, ProfileTypesRequest, ProfileTypesResponse,
    SelectHeatmapRequest, SelectHeatmapResponse, SelectMergeStacktracesRequest,
    SelectMergeStacktracesResponse, SelectSeriesRequest, SelectSeriesResponse, SeriesRequest,
    SeriesResponse,
};
use pulsus_profiles_wire::types_v1::{
    GoPgo, HeatmapSeries, HeatmapSlot, LabelPair, Labels, Location, Point, ProfileType, Series,
    StackTraceSelector,
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
    };
    assert_eq!(
        serde_json::to_string(&empty).expect("encode"),
        r#"{"flamegraph":{"names":["total"],"levels":[{"values":["0","0","0","0"]}]}}"#
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

/// W8 (issue #825): the heatmap request reads the query type by its name
/// or its number, a `double` as a number or a string, and an `optional
/// int64` as a string.
#[test]
fn w8_the_heatmap_request_reads_either_form() {
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

/// W9 (issue #825): a slot's `yMin` is written as the reference writes a
/// double, each integral one as an integer; `counts` as numbers; zeros in
/// both kept; defaults omitted.
#[test]
fn w9_a_slot_is_written_as_the_reference_writes_it() {
    let slot = HeatmapSlot {
        timestamp: 1_794_819_600_000,
        y_min: vec![0.0, 2.5],
        counts: vec![1, 0],
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
