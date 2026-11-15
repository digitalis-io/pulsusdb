//! The pprof model: a push's profiles become the rows of
//! [`ParsedProfileLanding`]. Test-first stub: the functions below return
//! nothing yet.

use pulsus_profiles_wire::google_v1::Profile;

use crate::ingest::profiles::{ParsedProfileLanding, ProfileInput, ProfileRefusal};

/// `validation.max-profile-size-bytes`.
pub const MAX_PROFILE_SIZE_BYTES: u64 = 4 * 1024 * 1024;

/// Stub.
pub fn decode_pprof(_bytes: &[u8], _limit: u64) -> Result<Profile, String> {
    Err(String::new())
}

/// Stub.
pub fn parse_profiles(
    _inputs: Vec<ProfileInput>,
    _now_ms: i64,
) -> Result<ParsedProfileLanding, ProfileRefusal> {
    Ok(ParsedProfileLanding::default())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;

    use prost::Message;
    use pulsus_profiles_wire::google_v1::{
        Function, Label, Line, Location, Mapping, Profile, Sample, ValueType,
    };

    use super::*;
    use crate::ingest::profiles::{LandingStack, ParsedProfileLanding, ProfileInput};

    const NOW_MS: i64 = 1_800_000_000_000;
    const NOW_NS: i64 = NOW_MS * 1_000_000;

    /// Builds a `Profile` with an interned string table.
    struct Pb {
        p: Profile,
    }

    impl Pb {
        fn new() -> Pb {
            Pb {
                p: Profile {
                    string_table: vec![String::new()],
                    time_nanos: NOW_NS,
                    duration_nanos: 10_000_000_000,
                    ..Profile::default()
                },
            }
        }

        fn s(&mut self, v: &str) -> i64 {
            if let Some(i) = self.p.string_table.iter().position(|x| x == v) {
                return i as i64;
            }
            self.p.string_table.push(v.to_owned());
            (self.p.string_table.len() - 1) as i64
        }

        fn sample_type(&mut self, t: &str, u: &str) -> &mut Pb {
            let vt = ValueType {
                r#type: self.s(t),
                unit: self.s(u),
            };
            self.p.sample_type.push(vt);
            self
        }

        fn period(&mut self, t: &str, u: &str) -> &mut Pb {
            self.p.period_type = Some(ValueType {
                r#type: self.s(t),
                unit: self.s(u),
            });
            self
        }

        /// A function `id` named `name` in `file`.
        fn func(&mut self, id: u64, name: &str, file: &str) -> &mut Pb {
            let f = Function {
                id,
                name: self.s(name),
                system_name: 0,
                filename: self.s(file),
                start_line: 1,
            };
            self.p.function.push(f);
            self
        }

        /// A location `id` with one line per function id, innermost first.
        fn loc(&mut self, id: u64, mapping_id: u64, address: u64, funcs: &[u64]) -> &mut Pb {
            self.p.location.push(Location {
                id,
                mapping_id,
                address,
                line: funcs
                    .iter()
                    .map(|&f| Line {
                        function_id: f,
                        line: 10,
                    })
                    .collect(),
                is_folded: false,
            });
            self
        }

        fn sample(&mut self, locs: &[u64], values: &[i64], labels: &[(&str, &str)]) -> &mut Pb {
            let label = labels
                .iter()
                .map(|(k, v)| Label {
                    key: self.s(k),
                    str: self.s(v),
                    num: 0,
                    num_unit: 0,
                })
                .collect();
            self.p.sample.push(Sample {
                location_id: locs.to_vec(),
                value: values.to_vec(),
                label,
            });
            self
        }

        fn build(&mut self) -> Profile {
            self.p.clone()
        }
    }

    fn pairs(ls: &[(&str, &str)]) -> Vec<(String, String)> {
        ls.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn input(labels: &[(&str, &str)], profile: Profile) -> ProfileInput {
        ProfileInput {
            labels: pairs(labels),
            profile,
            decompressed_bytes: 1000,
            id: None,
            annotations: Vec::new(),
        }
    }

    const PUSH: &[(&str, &str)] = &[("__name__", "process_cpu"), ("service_name", "checkout")];
    const L: &str = r#"{__name__="process_cpu", service_name="checkout"}"#;

    /// §0's profile: a Go CPU profile with two sample types and two stacks.
    fn example_builder() -> Pb {
        let mut b = Pb::new();
        b.sample_type("samples", "count")
            .sample_type("cpu", "nanoseconds")
            .period("cpu", "nanoseconds")
            .func(1, "main", "main.go")
            .func(2, "a", "main.go")
            .func(3, "b", "main.go")
            .loc(1, 0, 0, &[1])
            .loc(2, 0, 0, &[2])
            .loc(3, 0, 0, &[3])
            .sample(&[2, 1], &[3, 30_000_000], &[])
            .sample(&[3, 1], &[1, 10_000_000], &[]);
        b
    }

    fn example() -> Profile {
        example_builder().build()
    }

    fn parse(inputs: Vec<ProfileInput>) -> ParsedProfileLanding {
        parse_profiles(inputs, NOW_MS).expect("the push is accepted")
    }

    fn parse_one(labels: &[(&str, &str)], p: Profile) -> ParsedProfileLanding {
        parse(vec![input(labels, p)])
    }

    fn refusal(labels: &[(&str, &str)], p: Profile) -> String {
        refusal_of(input(labels, p))
    }

    fn refusal_of(i: ProfileInput) -> String {
        match parse_profiles(vec![i], NOW_MS) {
            Err(r) => r.message,
            Ok(l) => format!("accepted with {} entries", l.entries.len()),
        }
    }

    fn labels_of(json: &str) -> BTreeMap<String, String> {
        serde_json::from_str(json).expect("labels are a JSON object of strings")
    }

    fn names(s: &LandingStack) -> Vec<&str> {
        s.names.iter().map(|n| &**n).collect()
    }

    /// A one-type profile whose function `k` is `f{k}` in `file`, with a
    /// location `k` on it, for `k` in `1..=n`.
    fn numbered(n: u64, file: &str) -> Pb {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds");
        for k in 1..=n {
            b.func(k, &format!("f{k}"), file).loc(k, 0, 0, &[k]);
        }
        b
    }

    #[test]
    fn a1_frames_are_root_first_and_a_locations_lines_are_reversed() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, "f", "x.c")
            .func(2, "g_inner", "x.c")
            .func(3, "g_outer", "x.c")
            .loc(1, 0, 0, &[1])
            .loc(2, 0, 0, &[2, 3])
            .sample(&[1, 2], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.stacks.len(), 1, "one stack");
        assert_eq!(names(&l.stacks[0]), ["g_outer", "g_inner", "f"]);
    }

    #[test]
    fn a2_a_stack_is_keyed_by_its_locations_not_its_names() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds").func(1, "f", "x.c");
        b.p.mapping.push(Mapping {
            id: 1,
            has_functions: false,
            ..Mapping::default()
        });
        b.loc(1, 1, 1, &[1])
            .loc(2, 1, 2, &[1])
            .sample(&[1], &[1], &[])
            .sample(&[2], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.stacks.len(), 2, "two stacks");
        assert_ne!(l.stacks[0].stack_id, l.stacks[1].stack_id);
        assert_eq!(l.stacks[0].node_ids, l.stacks[1].node_ids);
        assert_eq!(l.locations.len(), 2, "two locations");
    }

    #[test]
    fn a3_a_frame_id_hashes_its_parent() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, "main", "x.c")
            .func(2, "a", "x.c")
            .loc(1, 0, 0, &[1])
            .loc(2, 0, 0, &[2])
            .sample(&[2, 2, 1], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.stacks.len(), 1, "one stack");
        assert_eq!(names(&l.stacks[0]), ["main", "a", "a"]);
        let ids: std::collections::HashSet<u128> = l.stacks[0].node_ids.iter().copied().collect();
        assert_eq!(ids.len(), 3, "node ids {:?}", l.stacks[0].node_ids);
    }

    #[test]
    fn a4_zero_and_negative_samples_go_and_equal_stacks_sum() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, "main", "x.c")
            .loc(1, 0, 0, &[1]);
        for v in [0, -1, 5, 5] {
            b.sample(&[1], &[v], &[]);
        }
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.samples.len(), 1, "one sample row");
        assert_eq!(l.samples[0].value, 10);
        assert_eq!(l.entries.len(), 1, "one entry");
        assert_eq!(l.entries[0].total, 10);
    }

    #[test]
    fn a5_the_example_becomes_two_series_with_their_label_sets() {
        let l = parse_one(PUSH, example());
        let type_ids: Vec<&str> = l.series.iter().map(|s| &*s.type_id).collect();
        assert_eq!(
            type_ids,
            [
                "process_cpu:samples:count:cpu:nanoseconds",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
            ]
        );
        assert_eq!(
            l.series[0].labels,
            r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:samples:count:cpu:nanoseconds","__service_name__":"checkout","__type__":"samples","__unit__":"count","service_name":"checkout"}"#
        );
        assert_eq!(&*l.series[0].service, "checkout");
        assert_eq!(l.series[0].timestamp_ns, NOW_NS);
        assert!(!l.series[0].time_defaulted);
        let totals: Vec<i64> = l.entries.iter().map(|e| e.total).collect();
        assert_eq!(totals, [4, 40_000_000]);
        assert!(l.entries.iter().all(|e| e.duration_ns == 10_000_000_000));
        assert_eq!(l.samples.len(), 4, "sample rows");
        assert_eq!(l.stacks.len(), 2, "stacks");
        assert_eq!(l.locations.len(), 3, "locations");
        assert_eq!(names(&l.stacks[0]), ["main", "a"]);
        assert_eq!(names(&l.stacks[1]), ["main", "b"]);
        assert_eq!(l.total_rows(), 13);
    }

    #[test]
    fn a6_profile_id_is_the_span_and_push_labels_win() {
        let mut b = example_builder();
        b.p.sample.clear();
        b.sample(
            &[2, 1],
            &[3, 30],
            &[("profile_id", "00f0000000000001"), ("pod", "p1")],
        );
        let labels = [
            ("__name__", "process_cpu"),
            ("service_name", "checkout"),
            ("pod", "p0"),
        ];
        let l = parse_one(&labels, b.build());
        assert_eq!(l.samples.len(), 2, "one row per type");
        assert!(l.samples.iter().all(|s| s.span_id == 0x00f0_0000_0000_0001));
        assert_eq!(l.series.len(), 2, "two series");
        let set = labels_of(&l.series[0].labels);
        assert_eq!(set.get("pod").map(String::as_str), Some("p0"));
        assert!(!set.contains_key("span_id") && !set.contains_key("profile_id"));
    }

    #[test]
    fn a7_a_push_without_service_name_is_unknown_service() {
        let l = parse_one(&[("__name__", "process_cpu")], example());
        assert_eq!(l.series.len(), 2, "two series");
        assert_eq!(&*l.series[0].service, "unknown_service");
        let set = labels_of(&l.series[0].labels);
        assert_eq!(
            set.get("service_name").map(String::as_str),
            Some("unknown_service")
        );
    }

    #[test]
    fn a8_godeltaprof_names_are_rewritten() {
        let l = parse_one(
            &[("__name__", "godeltaprof_memory"), ("service_name", "s")],
            example(),
        );
        assert_eq!(l.series.len(), 2, "two series");
        let set = labels_of(&l.series[0].labels);
        assert_eq!(set.get("__name__").map(String::as_str), Some("memory"));
        assert_eq!(
            set.get("__name_replaced__").map(String::as_str),
            Some("godeltaprof_memory")
        );
        assert!(!set.contains_key("__delta__"));
    }

    #[test]
    fn a9_spans_are_not_merged() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, "main", "x.c")
            .loc(1, 0, 0, &[1])
            .sample(&[1], &[1], &[("span_id", "0000000000000001")])
            .sample(&[1], &[1], &[("span_id", "0000000000000002")]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.series.len(), 1, "one series");
        assert_eq!(l.samples.len(), 2, "one row per span");
        let spans: Vec<u64> = l.samples.iter().map(|s| s.span_id).collect();
        assert_eq!(spans, [1, 2]);
    }

    #[test]
    fn a10_a_profile_without_a_time_takes_the_receive_time() {
        let mut p = example();
        p.time_nanos = 0;
        let l = parse_one(PUSH, p);
        assert_eq!(l.series.len(), 2, "two series");
        assert_eq!(l.series[0].timestamp_ns, NOW_NS);
        assert!(l.series[0].time_defaulted);
    }

    fn window_message(t: &str) -> String {
        format!(
            "profile with labels '{L}' is outside of ingestion window (profile timestamp: {t}, \
             the ingestion window starts at 2027-01-15 07:00:00 +0000 UTC and ends at \
             2027-01-15 08:10:00 +0000 UTC)"
        )
    }

    #[test]
    fn a11_v1_empty_profile() {
        let mut p = example();
        p.sample_type.clear();
        assert_eq!(refusal(PUSH, p), "empty profile");
    }

    #[test]
    fn a11_v2_the_ingestion_window() {
        let at = |ms: i64| {
            let mut p = example();
            p.time_nanos = ms * 1_000_000;
            refusal(PUSH, p)
        };
        assert_eq!(
            at(NOW_MS - 3_600_000),
            window_message("2027-01-15 07:00:00 +0000 UTC")
        );
        assert_eq!(
            at(NOW_MS + 600_000),
            window_message("2027-01-15 08:10:00 +0000 UTC")
        );
        assert_eq!(at(NOW_MS - 3_599_999), "accepted with 2 entries");
    }

    #[test]
    fn a11_v3_size() {
        let mut i = input(PUSH, example());
        i.decompressed_bytes = 4_194_305;
        assert_eq!(
            refusal_of(i),
            format!(
                "the profile with labels '{L}' exceeds the size limit (max_profile_size_byte, \
                 actual: 4194305, limit: 4194304)"
            )
        );
    }

    #[test]
    fn a11_v4_samples() {
        let mut b = example_builder();
        for _ in 0..15_999 {
            b.sample(&[2, 1], &[1, 1], &[]);
        }
        assert_eq!(
            refusal(PUSH, b.build()),
            format!(
                "the profile with labels '{L}' exceeds the samples count limit \
                 (max_profile_stacktrace_samples, actual: 16001, limit: 16000)"
            )
        );
    }

    #[test]
    fn a11_v5_sample_labels() {
        let mut b = example_builder();
        let labels: Vec<(String, String)> = (0..101)
            .map(|i| (format!("k{i}"), "v".to_owned()))
            .collect();
        let refs: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (&**k, &**v)).collect();
        b.sample(&[2, 1], &[1, 1], &refs);
        assert_eq!(
            refusal(PUSH, b.build()),
            format!(
                "the profile with labels '{L}' exceeds the sample labels limit \
                 (max_profile_stacktrace_sample_labels, actual: 101, limit: 100)"
            )
        );
    }

    #[test]
    fn a11_v6_v7_zero_ids() {
        let mut b = example_builder();
        b.loc(0, 0, 0, &[1]);
        assert_eq!(refusal(PUSH, b.build()), "location id is 0");
        let mut b = example_builder();
        b.func(0, "z", "z.go");
        assert_eq!(refusal(PUSH, b.build()), "function id is 0");
    }

    #[test]
    fn a11_v8_value_length() {
        let mut b = example_builder();
        b.sample(&[2, 1], &[1], &[]);
        assert_eq!(refusal(PUSH, b.build()), "sample value length mismatch");
    }

    #[test]
    fn a11_v9_string_zero() {
        let mut p = example();
        p.string_table[0] = "x".to_owned();
        assert_eq!(refusal(PUSH, p), "string 0 should be empty string");
        let mut p = example();
        p.string_table.clear();
        assert_eq!(refusal(PUSH, p), "string 0 should be empty string");
    }

    #[test]
    fn a11_v10_string_indices() {
        let len = example().string_table.len() as i64;
        let check = |edit: &dyn Fn(&mut Profile), message: &str| {
            let mut p = example();
            edit(&mut p);
            assert_eq!(refusal(PUSH, p), message);
        };
        check(
            &|p| p.sample_type[0].r#type = len,
            "sample type type string index out of range",
        );
        check(
            &|p| {
                p.sample[0].label.push(Label {
                    key: len,
                    str: 1,
                    num: 0,
                    num_unit: 0,
                })
            },
            "sample label string index out of range",
        );
        check(
            &|p| {
                p.sample[0].label.push(Label {
                    key: 1,
                    str: len,
                    num: 0,
                    num_unit: 0,
                })
            },
            "sample label string index out of range",
        );
        check(
            &|p| p.function[0].name = len,
            "function name string index out of range",
        );
        check(
            &|p| p.function[0].system_name = len,
            "function system name index string out of range",
        );
        check(
            &|p| p.function[0].filename = len,
            "function file name string index out of range",
        );
        check(
            &|p| {
                p.mapping.push(Mapping {
                    id: 1,
                    filename: len,
                    ..Mapping::default()
                })
            },
            "mapping file name string index out of range",
        );
        check(
            &|p| {
                p.mapping.push(Mapping {
                    id: 1,
                    build_id: len,
                    ..Mapping::default()
                })
            },
            "mapping build id string index out of range",
        );
    }

    #[test]
    fn a11_v11_sample_type_with_a_dash() {
        let mut b = example_builder();
        let t = b.s("cpu-x");
        b.p.sample_type[1].r#type = t;
        assert_eq!(refusal(PUSH, b.build()), "sample type contains -");
    }

    #[test]
    fn a11_l1_too_many_labels() {
        let mut labels = pairs(PUSH);
        for i in 0..29 {
            labels.push((format!("l{i:02}"), "v".to_owned()));
        }
        let refs: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (&**k, &**v)).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        let text = format!(
            "{{{}}}",
            sorted
                .iter()
                .map(|(k, v)| format!("{k}=\"{v}\""))
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert_eq!(
            refusal(&refs, example()),
            format!("profile series '{text}' has 31 label names; limit 30")
        );
    }

    #[test]
    fn a11_l2_no_metric_name() {
        assert_eq!(
            refusal(&[("service_name", "checkout")], example()),
            r#"invalid labels '{service_name="checkout"}' with error: invalid metric name"#
        );
    }

    #[test]
    fn a11_l3_long_name_and_value() {
        let long_name = "n".repeat(1025);
        assert_eq!(
            refusal(
                &[
                    ("__name__", "process_cpu"),
                    ("service_name", "checkout"),
                    (&long_name, "v")
                ],
                example()
            ),
            format!(
                "profile with labels '{{__name__=\"process_cpu\", {long_name}=\"v\", \
                 service_name=\"checkout\"}}' has label name too long: '{long_name}'"
            )
        );
        let long_value = "v".repeat(2049);
        assert_eq!(
            refusal(
                &[
                    ("__name__", "process_cpu"),
                    ("pod", &long_value),
                    ("service_name", "checkout")
                ],
                example()
            ),
            format!(
                "profile with labels '{{__name__=\"process_cpu\", pod=\"{long_value}\", \
                 service_name=\"checkout\"}}' has label value too long: '{long_value}'"
            )
        );
    }

    #[test]
    fn a11_l4_empty_label_name() {
        assert_eq!(
            refusal(
                &[
                    ("__name__", "process_cpu"),
                    ("service_name", "checkout"),
                    ("", "x")
                ],
                example()
            ),
            r#"invalid labels '{="x", __name__="process_cpu", service_name="checkout"}' with error: invalid label name ''"#
        );
    }

    #[test]
    fn a11_l5_duplicate_label_name() {
        assert_eq!(
            refusal(
                &[
                    ("__name__", "process_cpu"),
                    ("service_name", "checkout"),
                    ("pod", "a"),
                    ("pod", "b")
                ],
                example()
            ),
            r#"profile with labels '{__name__="process_cpu", pod="a", pod="b", service_name="checkout"}' has duplicate label name: 'pod'"#
        );
    }

    #[test]
    fn a12_a_deep_stack_keeps_its_root_side() {
        let mut b = numbered(1001, "x.c");
        let all: Vec<u64> = (1..=1001).collect();
        b.sample(&all, &[1], &[]);
        let l = parse_one(PUSH, b.build());
        let mut r = numbered(1001, "x.c");
        r.sample(&all[1..], &[1], &[]);
        let expected = parse_one(PUSH, r.build());
        assert_eq!(l.stacks.len(), 1, "one stack");
        assert_eq!(l.stacks[0].location_ids.len(), 1000);
        assert_eq!(expected.stacks.len(), 1, "one reference stack");
        assert_eq!(l.stacks[0].location_ids, expected.stacks[0].location_ids);
    }

    #[test]
    fn a13_a_long_symbol_keeps_its_last_bytes() {
        let long = format!("{}é", "x".repeat(65_536));
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, &long, "x.c")
            .loc(1, 0, 0, &[1])
            .sample(&[1], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.stacks.len(), 1, "one stack");
        let name = &*l.stacks[0].names[0];
        assert_eq!(name.len(), 65_535);
        assert!(name.ends_with('é'));
        assert_eq!(name, &long[long.len() - 65_535..]);

        // The cut lands inside `é`: it moves forward to the next character.
        let long = format!("xé{}", "x".repeat(65_534));
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, &long, "x.c")
            .loc(1, 0, 0, &[1])
            .sample(&[1], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.stacks.len(), 1, "one stack");
        assert_eq!(&*l.stacks[0].names[0], "x".repeat(65_534));
    }

    #[test]
    fn a14_go_type_parameters_are_dropped_for_go_only() {
        let build = |file: &str| {
            let mut b = Pb::new();
            b.sample_type("cpu", "nanoseconds")
                .func(1, "pkg.(*T[go.shape.int]).Method", file)
                .loc(1, 0, 0, &[1])
                .sample(&[1], &[1], &[]);
            parse_one(PUSH, b.build())
        };
        let go = build("x.go");
        assert_eq!(go.stacks.len(), 1, "one stack");
        assert_eq!(names(&go.stacks[0]), ["pkg.(*T).Method"]);
        let py = build("x.py");
        assert_eq!(py.stacks.len(), 1, "one stack");
        assert_eq!(names(&py.stacks[0]), ["pkg.(*T[go.shape.int]).Method"]);
    }

    #[test]
    fn a15_a_truncated_go_stack_is_repaired_from_a_longer_one() {
        let s: Vec<u64> = (1..=64).collect();
        let mut r: Vec<u64> = vec![100];
        r.extend(49..=72);
        let mut b = numbered(100, "x.go");
        b.sample(&s, &[1], &[]).sample(&r, &[1], &[]);
        let l = parse_one(PUSH, b.build());

        let reference = |locs: &[u64]| {
            // One sample alone: nothing to repair it from.
            let mut b = numbered(100, "x.go");
            b.sample(locs, &[1], &[]);
            let l = parse_one(PUSH, b.build());
            assert_eq!(l.stacks.len(), 1, "one reference stack");
            l.stacks[0].location_ids.clone()
        };
        let full: Vec<u64> = (1..=72).collect();
        assert_eq!(l.stacks.len(), 2, "two stacks");
        let by_len = |n: usize| {
            l.stacks
                .iter()
                .find(|st| st.location_ids.len() == n)
                .map(|st| st.location_ids.clone())
        };
        assert_eq!(by_len(72), Some(reference(&full)), "S is repaired");
        assert_eq!(by_len(25), Some(reference(&r)), "R is unchanged");
    }

    #[test]
    fn a16_references_follow_the_reference_rules() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds").func(1, "f", "x.c");
        let file = b.s("libx.so");
        b.p.mapping.push(Mapping {
            id: 1,
            memory_start: 4096,
            memory_limit: 8192,
            filename: file,
            has_functions: true,
            ..Mapping::default()
        });
        b.loc(2, 0, 0, &[]) // no lines, address 0
            .loc(3, 1, 77, &[1])
            .sample(&[9], &[1], &[])
            .sample(&[2], &[1], &[])
            .sample(&[3], &[1], &[]);
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.samples.len(), 1, "only the third sample gives a row");
        assert_eq!(l.locations.len(), 1, "one location");
        let loc = &l.locations[0];
        assert_eq!((loc.mapping_start, loc.address), (0, 0));
        assert_eq!(&*loc.mapping_file, "libx.so");
    }

    #[test]
    fn a17_decode_reads_raw_and_gzipped_bodies_and_bounds_the_output() {
        let p = example();
        let raw = p.encode_to_vec();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&raw).expect("gzip");
        let gz = gz.finish().expect("gzip");
        assert_ne!(raw, gz);
        assert_eq!(decode_pprof(&raw, MAX_PROFILE_SIZE_BYTES), Ok(p.clone()));
        assert_eq!(decode_pprof(&gz, MAX_PROFILE_SIZE_BYTES), Ok(p));
        let big = vec![0u8; 4_194_305];
        assert_eq!(
            decode_pprof(&big, MAX_PROFILE_SIZE_BYTES),
            Err("decompressed size exceeds maximum allowed size of 4194304 bytes".to_owned())
        );
    }

    #[test]
    fn a19_stacks_are_distinct_across_the_push() {
        let mut b = Pb::new();
        b.sample_type("cpu", "nanoseconds")
            .func(1, "main", "x.c")
            .loc(1, 0, 0, &[1])
            .sample(&[1], &[1], &[]);
        let l = parse(vec![input(PUSH, b.build()), input(PUSH, b.build())]);
        assert_eq!(l.stacks.len(), 1, "one stack");
        assert_eq!(l.locations.len(), 1, "one location");
        assert_eq!(l.samples.len(), 2, "two sample rows");
        assert_ne!(l.samples[0].entry, l.samples[1].entry);
    }

    #[test]
    fn a20_a_refusal_in_any_input_refuses_the_push() {
        let mut bad = example();
        bad.sample_type.clear();
        let r = parse_profiles(vec![input(PUSH, example()), input(PUSH, bad)], NOW_MS);
        assert_eq!(
            r,
            Err(ProfileRefusal {
                message: "empty profile".to_owned()
            })
        );
    }

    #[test]
    fn a21_entries_carry_their_input_and_its_id() {
        let id = [7u8; 16];
        let mut first = input(PUSH, example());
        first.id = Some(id);
        let l = parse(vec![first, input(PUSH, example())]);
        let got: Vec<(u32, Option<[u8; 16]>)> =
            l.entries.iter().map(|e| (e.input, e.profile_id)).collect();
        assert_eq!(got, [(0, Some(id)), (0, Some(id)), (1, None), (1, None)]);
    }

    #[test]
    fn a22_wall_cpu_is_renamed_only_when_the_push_carries_the_type() {
        let profile = || {
            let mut b = Pb::new();
            b.sample_type("cpu", "nanoseconds")
                .sample_type("wall", "nanoseconds")
                .period("wall", "nanoseconds")
                .func(1, "main", "x.c")
                .loc(1, 0, 0, &[1])
                .sample(&[1], &[1, 2], &[]);
            b.build()
        };
        let a = parse_one(
            &[
                ("__name__", "wall"),
                ("__type__", "cpu"),
                ("service_name", "s"),
            ],
            profile(),
        );
        let ids: Vec<&str> = a.series.iter().map(|s| &*s.type_id).collect();
        assert_eq!(
            ids,
            [
                "process_cpu:cpu:nanoseconds:wall:nanoseconds",
                "process_cpu:wall:nanoseconds:wall:nanoseconds"
            ]
        );
        for (s, ty) in a.series.iter().zip(["cpu", "wall"]) {
            let set = labels_of(&s.labels);
            assert_eq!(set.get("__name__").map(String::as_str), Some("process_cpu"));
            assert_eq!(
                set.get("__name_replaced__").map(String::as_str),
                Some("wall")
            );
            assert_eq!(set.get("__type__").map(String::as_str), Some(ty));
        }

        let b = parse_one(&[("__name__", "wall"), ("service_name", "s")], profile());
        let ids: Vec<&str> = b.series.iter().map(|s| &*s.type_id).collect();
        assert_eq!(
            ids,
            [
                "wall:cpu:nanoseconds:wall:nanoseconds",
                "wall:wall:nanoseconds:wall:nanoseconds"
            ]
        );
        for s in &b.series {
            let set = labels_of(&s.labels);
            assert_eq!(set.get("__name__").map(String::as_str), Some("wall"));
            assert!(!set.contains_key("__name_replaced__"));
        }
    }

    #[test]
    fn a23_out_of_range_reads_are_empty_not_refused() {
        let mut b = example_builder();
        let pod = b.s("pod");
        b.p.sample[0].label.push(Label {
            key: pod,
            str: -1,
            num: 0,
            num_unit: 0,
        });
        let len = b.p.string_table.len() as i64;
        b.p.period_type.as_mut().expect("period").r#type = len;
        let l = parse_one(PUSH, b.build());
        assert_eq!(l.series.len(), 2, "two series");
        assert_eq!(
            &*l.series[0].type_id,
            "process_cpu:samples:count::nanoseconds"
        );
        let set = labels_of(&l.series[0].labels);
        assert!(!set.contains_key("pod"));
        assert!(!set.contains_key("__period_type__"));
    }
}
