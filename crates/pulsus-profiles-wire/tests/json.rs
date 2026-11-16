//! W2 and W3 (issue #821): the query messages' proto3 JSON mapping. Out:
//! lowerCamelCase names, a field at its default omitted. In: a field by its
//! JSON name or its proto name, an `int64` as a number or a decimal string,
//! an unknown field ignored.

use pulsus_profiles_wire::querier_v1::{
    ProfileTypesRequest, ProfileTypesResponse, SeriesRequest, SeriesResponse,
};
use pulsus_profiles_wire::types_v1::{LabelPair, Labels, ProfileType};

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
