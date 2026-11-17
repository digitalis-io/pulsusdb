//! Speedscope JSON (`format=speedscope`), read as the reference reads it
//! (`pkg/og/convert/speedscope/parser.go`, `units.go`, `json.go`), with the
//! three value defects of issue #818's Q3 corrected.

use super::LegacyError;
use crate::ingest::profiles::ProfileInput;
use crate::protocols::profile_push::LegacyQuery;

/// Every profile of the file, merged and parsed before any is built.
pub fn parse_speedscope(
    _body: &[u8],
    _query: &LegacyQuery,
) -> Result<Vec<ProfileInput>, LegacyError> {
    Err(LegacyError::Parse("not built".to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    fn parse(body: &str, name: &str) -> Result<Vec<ProfileInput>, LegacyError> {
        parse_speedscope(body.as_bytes(), &query(name, 100))
    }

    /// Each input's `__name__`, sample type, `profile_name` and stacks.
    type Summary = (Option<String>, String, Option<String>, Stacks);

    fn summary(
        inputs: Result<Vec<ProfileInput>, LegacyError>,
    ) -> Result<Vec<Summary>, LegacyError> {
        inputs.map(|inputs| {
            inputs
                .iter()
                .map(|i| {
                    (
                        label(i, "__name__").map(str::to_string),
                        sample_type(i),
                        label(i, "profile_name").map(str::to_string),
                        stacks(i),
                    )
                })
                .collect()
        })
    }

    fn one_profile(profile: &str) -> String {
        format!(
            r#"{{"$schema":"https://www.speedscope.app/file-format-schema.json",
                "shared":{{"frames":[{{"name":"a"}},{{"name":"b"}}]}},
                "profiles":[{profile}]}}"#
        )
    }

    fn stacks_one(profile: &str) -> Result<Stacks, LegacyError> {
        parse(&one_profile(profile), "app").map(|inputs| {
            assert_eq!(inputs.len(), 1);
            stacks(&inputs[0])
        })
    }

    #[test]
    fn s1_an_evented_profile_reads() {
        assert_eq!(
            summary(parse(SIMPLE, "foo")),
            Ok(vec![(
                Some("process_cpu".to_string()),
                "cpu/nanoseconds".to_string(),
                Some("simple.txt".to_string()),
                map(&[
                    (&["a", "b"], 50_000_000),
                    (&["a", "b", "c"], 50_000_000),
                    (&["a", "b", "d"], 40_000_000),
                ])
            )])
        );
    }

    #[test]
    fn s2_two_sampled_profiles_are_two_inputs() {
        let inputs = parse(&two_sampled(), "foo{x=y}");
        let want = |profile: &str| {
            (
                Some("wall".to_string()),
                "wall/nanoseconds".to_string(),
                Some(profile.to_string()),
                map(&[
                    (&["a", "b"], 5_000_000_000),
                    (&["a", "b", "c"], 5_000_000_000),
                    (&["a", "b", "d"], 4_000_000_000),
                ]),
            )
        };
        let labels: Result<Vec<_>, _> = inputs.as_ref().map(|inputs| {
            inputs
                .iter()
                .map(|i| {
                    (
                        label(i, "service_name").map(str::to_string),
                        label(i, "x").map(str::to_string),
                    )
                })
                .collect::<Vec<_>>()
        });
        let foo = (Some("foo".to_string()), Some("y".to_string()));
        assert_eq!(labels.map_err(|e| e.clone()), Ok(vec![foo.clone(), foo]));
        assert_eq!(summary(inputs), Ok(vec![want("one"), want("two")]));
    }

    /// The reference's `duplicates.speedscope.json`: one profile listed three
    /// times.
    #[test]
    fn s3_equal_profiles_merge() {
        let body = speedscope_file(&[
            sampled_profile("one", 14),
            sampled_profile("one", 14),
            sampled_profile("one", 52),
        ]);
        assert_eq!(
            summary(parse(&body, "foo{x=y}")),
            Ok(vec![(
                Some("process_cpu".to_string()),
                "cpu/nanoseconds".to_string(),
                Some("one".to_string()),
                map(&[
                    (&["a", "b"], 15_000_000_000),
                    (&["a", "b", "c"], 15_000_000_000),
                    (&["a", "b", "d"], 12_000_000_000),
                ])
            )])
        );
    }

    #[test]
    fn s4_each_refusal_carries_the_reference_text() {
        let schema = r#"{"$schema":"x","profiles":[]}"#.to_string();
        let evented = |events: &str| {
            one_profile(&format!(
                r#"{{"type":"evented","name":"p","unit":"none","startValue":0,"events":[{events}]}}"#
            ))
        };
        let sampled = |samples: &str, weights: &str| {
            one_profile(&format!(
                r#"{{"type":"sampled","name":"p","unit":"none","samples":{samples},"weights":{weights}}}"#
            ))
        };
        let cases = [
            (schema, "Unknown schema: x"),
            (
                one_profile(r#"{"type":"sampled","name":"p","unit":"furlong"}"#),
                "invalid profile unit: unknown unit: furlong",
            ),
            (
                one_profile(r#"{"type":"x","name":"p","unit":"none"}"#),
                "Profile type x not supported",
            ),
            (
                evented(r#"{"type":"O","frame":0,"at":2},{"type":"O","frame":1,"at":1}"#),
                "Events out of order, 1.000000 < 2.000000",
            ),
            (
                evented(r#"{"type":"O","frame":0,"at":0},{"type":"C","frame":1,"at":1}"#),
                "Closing non-open frame 1",
            ),
            (
                evented(r#"{"type":"C","frame":0,"at":1}"#),
                "No stack to close at 1.000000",
            ),
            (
                evented(r#"{"type":"O","frame":2,"at":1}"#),
                "Invalid frame 2",
            ),
            (
                evented(r#"{"type":"X","frame":0,"at":1}"#),
                "Unknown event type X",
            ),
            (
                sampled("[[0]]", "[]"),
                "Unequal lengths of samples and weights: 1 != 0",
            ),
            (sampled("[[0]]", "[-1]"), "Negative weight -1.000000"),
            (sampled("[[0, 2]]", "[1]"), "Invalid frame 2"),
        ];
        for (body, message) in cases {
            assert_eq!(
                parse(&body, "app").map(|i| i.len()),
                Err(LegacyError::Parse(message.to_string())),
                "{body}"
            );
        }
    }

    /// Q3: values are computed before truncation, an open gap takes the
    /// precision multiplier, and `nanoseconds` stores its weights.
    #[test]
    fn s5_speedscope_values_are_computed_correctly() {
        assert_eq!(
            stacks_one(
                r#"{"type":"sampled","name":"p","unit":"seconds","samples":[[0]],"weights":[0.5]}"#
            ),
            Ok(map(&[(&["a"], 500_000_000)])),
            "a"
        );
        assert_eq!(
            stacks_one(
                r#"{"type":"evented","name":"p","unit":"none","startValue":0,"events":[
                    {"type":"O","frame":0,"at":0},{"type":"O","frame":1,"at":3},
                    {"type":"C","frame":1,"at":5},{"type":"C","frame":0,"at":6}]}"#
            ),
            Ok(map(&[(&["a"], 40_000_000), (&["a", "b"], 20_000_000)])),
            "b"
        );
        assert_eq!(
            stacks_one(
                r#"{"type":"sampled","name":"p","unit":"nanoseconds","samples":[[0]],"weights":[1000]}"#
            ),
            Ok(map(&[(&["a"], 1000)])),
            "c"
        );
    }
}
