//! Issue #824: `SelectSeries` over the Connect protocol, and
//! `GET /api/profiles/v1/select_series`. Not written yet.

use pulsus_profiles_wire::querier_v1::SelectSeriesRequest;

use super::connect::ConnectError;

/// The exemplar types served. Not written yet.
fn check_exemplar_type(v: i32) -> Result<(), ConnectError> {
    let _ = v;
    Ok(())
}

/// The `GET` route's parameters as the Connect request. Not written yet.
fn series_request(
    pairs: &[(String, String)],
    now: i64,
) -> Result<SelectSeriesRequest, ConnectError> {
    let _ = (pairs, now);
    Ok(SelectSeriesRequest::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles_api::connect::Code;
    use pulsus_profiles_wire::types_v1::TimeSeriesAggregationType;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, val)| (k.to_string(), val.to_string()))
            .collect()
    }

    /// The `GET` route splits `query` at its first `{`, reads `step` as
    /// seconds, `groupBy[]` in order, `aggregation` `avg` and `limit`, and
    /// refuses a `step` or a `limit` that is not a number.
    #[test]
    fn the_get_route_reads_its_parameters() {
        let m = series_request(
            &pairs(&[
                ("query", r#"a:b:c:d:e{service_name="x"}"#),
                ("from", "1000"),
                ("until", "2000"),
                ("step", "15"),
                ("groupBy[]", "pod"),
                ("groupBy[]", "service_name"),
                ("aggregation", "avg"),
                ("limit", "1"),
            ]),
            0,
        )
        .expect("a request");
        assert_eq!(m.profile_type_id, "a:b:c:d:e");
        assert_eq!(m.label_selector, r#"{service_name="x"}"#);
        assert_eq!((m.start, m.end), (1_000_000, 2_000_000));
        assert_eq!(m.step, 15.0);
        assert_eq!(m.group_by, ["pod", "service_name"]);
        assert_eq!(
            m.aggregation,
            Some(TimeSeriesAggregationType::Average as i32)
        );
        assert_eq!(m.limit, Some(1));
        let m = series_request(&pairs(&[("query", "a:b:c:d:e"), ("aggregation", "max")]), 0)
            .expect("a request");
        assert_eq!(m.label_selector, "{}");
        assert_eq!(m.aggregation, Some(TimeSeriesAggregationType::Sum as i32));
        assert_eq!((m.step, m.limit), (0.0, None));
        for (key, text) in [
            ("step", r#"invalid step "x""#),
            ("limit", r#"invalid limit "x""#),
        ] {
            let e = series_request(&pairs(&[(key, "x")]), 0).expect_err("refused");
            assert_eq!((e.code, e.message.as_str()), (Code::InvalidArgument, text));
        }
    }

    /// D9: unspecified and `NONE` are served; `INDIVIDUAL` and `SPAN` are
    /// not; another number is refused.
    #[test]
    fn the_exemplar_type_is_checked() {
        for v in [0, 1] {
            assert!(check_exemplar_type(v).is_ok(), "{v}");
        }
        for (v, code, text) in [
            (2, Code::Unimplemented, "exemplars are not served yet"),
            (
                3,
                Code::Unimplemented,
                "exemplar type span is not implemented",
            ),
            (7, Code::InvalidArgument, "unknown exemplar type: 7"),
        ] {
            let e = check_exemplar_type(v).expect_err("refused");
            assert_eq!((e.code, e.message.as_str()), (code, text), "{v}");
        }
    }
}
