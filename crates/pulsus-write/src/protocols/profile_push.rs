//! The two profile push transports' request rules, ahead of the pprof model
//! (`profile_model`): the push route's series and samples become
//! [`ProfileInput`]s ([`push_inputs`]), and `/ingest`'s query parameters,
//! multipart form and pprof body become one ([`legacy_pprof_input`]).
//! Pure functions, no I/O.

use std::collections::HashMap;

use pulsus_profiles_wire::connect::Code;
use pulsus_profiles_wire::google_v1::Profile;
use pulsus_profiles_wire::push_v1::PushRequest;

use crate::ingest::profiles::ProfileInput;

/// A label set, in first-seen order.
pub type Labels = Vec<(String, String)>;

/// `/ingest`'s refusal of a form that carries `prev_profile`.
pub const PREV_PROFILE_MESSAGE: &str = "";

/// Why the push route refused a push before the model ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushInputError {
    pub code: Code,
    pub message: String,
}

/// The `sample_type_config` form part: each sample type's display name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SampleTypeConfig {
    display_names: HashMap<String, String>,
}

impl SampleTypeConfig {
    /// Reads the part's JSON object.
    pub fn parse(_bytes: &[u8]) -> Result<Self, String> {
        Ok(SampleTypeConfig::default())
    }
}

/// The multipart form's parts `/ingest` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Multipart {
    pub profile: Option<Vec<u8>>,
    pub sample_type_config: Option<SampleTypeConfig>,
}

/// `/ingest`'s query parameters, as the pprof path reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyQuery {
    /// `name`, parsed.
    pub name: Labels,
    /// `from`, in nanoseconds, when given.
    pub from_ns: Option<i64>,
    /// `spyName`, `unknown` when absent.
    pub spy_name: String,
    /// `format`, when given.
    pub format: Option<String>,
}

/// Stub.
pub fn push_inputs(_req: PushRequest) -> Result<Vec<ProfileInput>, PushInputError> {
    Ok(Vec::new())
}

/// Stub.
pub fn parse_name(_name: &str) -> Result<Labels, String> {
    Ok(Vec::new())
}

/// Stub.
pub fn parse_at_time(_s: &str, _now_ns: i64) -> i64 {
    0
}

/// Stub.
pub fn parse_multipart(_content_type: &str, _body: &[u8]) -> Result<Multipart, String> {
    Ok(Multipart::default())
}

/// Stub.
pub fn series_name(_profile: &Profile, _config: Option<&SampleTypeConfig>) -> String {
    String::new()
}

/// Stub.
pub fn legacy_pprof_input(
    profile: Profile,
    _query: &LegacyQuery,
    _config: Option<&SampleTypeConfig>,
) -> ProfileInput {
    ProfileInput {
        labels: Vec::new(),
        profile,
        decompressed_bytes: 0,
        id: None,
        annotations: Vec::new(),
    }
}
#[cfg(test)]
pub(crate) mod fixture {
    //! Part A §0's profile, shared by this module's tests and the route
    //! tests in `ingest/profiles_http.rs`.

    use std::io::Write;

    use prost::Message;
    use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};

    /// Builds a `Profile` with an interned string table.
    pub(crate) struct Pb {
        pub(crate) p: Profile,
    }

    impl Pb {
        pub(crate) fn new(time_nanos: i64) -> Pb {
            Pb {
                p: Profile {
                    string_table: vec![String::new()],
                    time_nanos,
                    duration_nanos: 10_000_000_000,
                    ..Profile::default()
                },
            }
        }

        pub(crate) fn s(&mut self, v: &str) -> i64 {
            if let Some(i) = self.p.string_table.iter().position(|x| x == v) {
                return i as i64;
            }
            self.p.string_table.push(v.to_owned());
            (self.p.string_table.len() - 1) as i64
        }

        pub(crate) fn sample_type(&mut self, t: &str, u: &str) -> &mut Pb {
            let vt = ValueType {
                r#type: self.s(t),
                unit: self.s(u),
            };
            self.p.sample_type.push(vt);
            self
        }

        pub(crate) fn period(&mut self, t: &str, u: &str) -> &mut Pb {
            self.p.period_type = Some(ValueType {
                r#type: self.s(t),
                unit: self.s(u),
            });
            self
        }

        pub(crate) fn func(&mut self, id: u64, name: &str, file: &str) -> &mut Pb {
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

        pub(crate) fn loc(&mut self, id: u64, funcs: &[u64]) -> &mut Pb {
            self.p.location.push(Location {
                id,
                mapping_id: 0,
                address: 0,
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

        pub(crate) fn sample(&mut self, locs: &[u64], values: &[i64]) -> &mut Pb {
            self.p.sample.push(Sample {
                location_id: locs.to_vec(),
                value: values.to_vec(),
                label: Vec::new(),
            });
            self
        }

        pub(crate) fn build(&mut self) -> Profile {
            self.p.clone()
        }
    }

    /// Now, in nanoseconds since the epoch.
    pub(crate) fn now_ns() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_nanos() as i64
    }

    /// Part A §0's Go CPU profile at `time_nanos`: two sample types, two
    /// stacks.
    pub(crate) fn example_at(time_nanos: i64) -> Profile {
        let mut b = Pb::new(time_nanos);
        b.sample_type("samples", "count")
            .sample_type("cpu", "nanoseconds")
            .period("cpu", "nanoseconds")
            .func(1, "main", "main.go")
            .func(2, "a", "main.go")
            .func(3, "b", "main.go")
            .loc(1, &[1])
            .loc(2, &[2])
            .loc(3, &[3])
            .sample(&[2, 1], &[3, 30_000_000])
            .sample(&[3, 1], &[1, 10_000_000]);
        b.build()
    }

    /// `P`: the example at now.
    pub(crate) fn p() -> Profile {
        example_at(now_ns())
    }

    pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(bytes).expect("gzip");
        gz.finish().expect("gzip")
    }

    /// `PB`: `P`'s gzip bytes.
    pub(crate) fn pb() -> Vec<u8> {
        gzip(&p().encode_to_vec())
    }

    /// `P` with no sample types.
    pub(crate) fn no_sample_types() -> Profile {
        let mut p = p();
        p.sample_type.clear();
        p
    }

    /// A multipart body with each `(name, filename, bytes)` part; a `None`
    /// filename sends the part as a plain field.
    pub(crate) fn multipart(boundary: &str, parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, filename, bytes) in parts {
            out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            match filename {
                Some(f) => out.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{name}\"; filename=\"{f}\"\r\n\
                         Content-Type: application/octet-stream\r\n\r\n"
                    )
                    .as_bytes(),
                ),
                None => out.extend_from_slice(
                    format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
                ),
            }
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use pulsus_profiles_wire::google_v1::Profile;
    use pulsus_profiles_wire::push_v1::{PushRequest, RawProfileSeries, RawSample};
    use pulsus_profiles_wire::types_v1::{LabelPair, ProfileAnnotation};

    use super::fixture::{self, Pb};
    use super::*;
    use crate::protocols::profile_model::SPY_LABEL;

    fn pairs(ls: &[(&str, &str)]) -> Vec<(String, String)> {
        ls.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn query(name: &str, from_ns: Option<i64>, spy: &str) -> LegacyQuery {
        LegacyQuery {
            name: parse_name(name).expect("a valid name"),
            from_ns,
            spy_name: spy.to_string(),
            format: None,
        }
    }

    fn with_types(types: &[&str]) -> Profile {
        let mut b = Pb::new(fixture::now_ns());
        for t in types {
            b.sample_type(t, "count");
        }
        b.build()
    }

    /// Each line's function name, location by location.
    fn line_names(p: &Profile) -> Vec<String> {
        p.location
            .iter()
            .flat_map(|l| l.line.iter())
            .map(|line| {
                let f = p
                    .function
                    .iter()
                    .find(|f| f.id == line.function_id)
                    .expect("the line's function");
                p.string_table[f.name as usize].clone()
            })
            .collect()
    }

    #[test]
    fn u1_the_name_parameter_is_the_reference_label_set() {
        assert_eq!(
            parse_name("app.cpu{env=prod}"),
            Ok(pairs(&[("__name__", "app.cpu"), ("env", "prod")]))
        );
        assert_eq!(
            parse_name(" a { k = v , j=w }"),
            Ok(pairs(&[("__name__", "a"), ("k", "v"), ("j", "w")]))
        );
        assert_eq!(parse_name(""), Err("service name is required".to_string()));
        assert_eq!(
            parse_name("a!b"),
            Err("invalid service name: a!b: character is not allowed: '!'".to_string())
        );
        assert_eq!(
            parse_name("app{x-y=1}"),
            Err("invalid label name: x-y: character is not allowed: '-'".to_string())
        );
    }

    #[test]
    fn u2_the_series_name_follows_the_sample_types_and_the_config() {
        let config = SampleTypeConfig::parse(br#"{"contentions":{"display-name":"block_count"}}"#)
            .expect("a valid config");
        let cases: [(&[&str], Option<&SampleTypeConfig>, &str); 6] = [
            (&["samples", "cpu"], None, "process_cpu"),
            (&["contentions", "delay"], None, "mutex"),
            (&["contentions", "delay"], Some(&config), "block"),
            (&["alloc_objects"], None, "memory"),
            (&["cpu", "wall"], None, "wall"),
            (&["x"], None, "x"),
        ];
        for (types, config, want) in cases {
            assert_eq!(
                series_name(&with_types(types), config),
                want,
                "{types:?} with {config:?}"
            );
        }
    }

    #[test]
    fn u3_the_legacy_labels_are_the_reference_labels_in_order() {
        let input = legacy_pprof_input(
            fixture::p(),
            &query("app.cpu{env=prod}", None, "unknown"),
            None,
        );
        assert_eq!(
            input.labels,
            pairs(&[
                ("__name__", "process_cpu"),
                ("__delta__", "false"),
                (SPY_LABEL, "unknown"),
                ("service_name", "app.cpu"),
                ("env", "prod"),
            ])
        );
        assert_eq!(input.id, None);
        assert!(input.annotations.is_empty());
        assert_eq!(input.decompressed_bytes, input.profile.encoded_len() as u64);

        let input = legacy_pprof_input(
            fixture::p(),
            &query("app{service_name=svc}", None, "unknown"),
            None,
        );
        let services: Vec<&str> = input
            .labels
            .iter()
            .filter(|(k, _)| k == "service_name")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(services, ["svc"]);
    }

    #[test]
    fn u4_a_time_in_1970_takes_from() {
        let time_of = |t: i64, from: Option<i64>| {
            legacy_pprof_input(fixture::example_at(t), &query("app", from, "unknown"), None)
                .profile
                .time_nanos
        };
        assert_eq!(time_of(0, None), 0);
        assert_eq!(
            time_of(1_700_000_000_000_000, Some(1_800_000_000 * 1_000_000_000)),
            1_800_000_000 * 1_000_000_000
        );
        let now = fixture::now_ns();
        assert_eq!(time_of(now, Some(1_800_000_000 * 1_000_000_000)), now);
    }

    #[test]
    fn u5_from_reads_as_the_reference_reads_it() {
        let now: i64 = 1_800_000_000_123_456_789;
        assert_eq!(parse_at_time("1800000000", now), 1_800_000_000_000_000_000);
        assert_eq!(
            parse_at_time("1800000000123", now),
            1_800_000_000_123_000_000
        );
        assert_eq!(
            parse_at_time("1800000000123456", now),
            1_800_000_000_123_456_000
        );
        assert_eq!(
            parse_at_time("1800000000123456789", now),
            1_800_000_000_123_456_789
        );
        // 2026-11-15T00:00:00Z.
        assert_eq!(
            parse_at_time("20261115", now),
            1_794_700_800 * 1_000_000_000
        );
        assert_eq!(parse_at_time("now-1h", now), now - 3_600 * 1_000_000_000);
    }

    #[test]
    fn u6_the_multipart_form_is_read_as_the_reference_reads_it() {
        let ct = "multipart/form-data; boundary=XyZ";
        let body = fixture::multipart(
            "XyZ",
            &[
                ("profile", Some("profile.pprof"), b"PPROF"),
                (
                    "sample_type_config",
                    Some("config.json"),
                    br#"{"samples":{"display-name":"cpu"}}"#,
                ),
            ],
        );
        let form = parse_multipart(ct, &body).expect("a valid form");
        assert_eq!(form.profile.as_deref(), Some(&b"PPROF"[..]));
        assert_eq!(
            form.sample_type_config,
            Some(SampleTypeConfig::parse(br#"{"samples":{"display-name":"cpu"}}"#).expect("valid"))
        );

        let body = fixture::multipart(
            "XyZ",
            &[
                ("profile", Some("profile.pprof"), b"PPROF"),
                ("prev_profile", Some("prev.pprof"), b"OLD"),
            ],
        );
        assert_eq!(
            parse_multipart(ct, &body).map(|f| f.profile),
            Err(PREV_PROFILE_MESSAGE.to_string())
        );
        assert!(PREV_PROFILE_MESSAGE.starts_with("unsupported client version. Please update "));

        let body = fixture::multipart("XyZ", &[("profile", None, b"PPROF")]);
        assert_eq!(parse_multipart(ct, &body).map(|f| f.profile), Ok(None));

        assert_eq!(
            parse_multipart("multipart/form-data", &body).map(|f| f.profile),
            Err("malformed multipart content type header".to_string())
        );
    }

    #[test]
    fn u7_scripting_spies_name_functions_by_file() {
        let profile = |file: &str| {
            let mut b = Pb::new(fixture::now_ns());
            b.sample_type("samples", "count")
                .func(1, "main", file)
                .loc(1, &[1])
                .sample(&[1], &[1]);
            b.build()
        };
        let names = |file: &str, spy: &str| {
            line_names(&legacy_pprof_input(profile(file), &query("app", None, spy), None).profile)
        };
        assert_eq!(names("app.py", "pyspy"), ["app.py main"]);
        assert_eq!(names("/x/app.py", "pyspy"), ["main"]);
        assert_eq!(names("/x/app.py", "rbspy"), ["/x/app.py main"]);
        assert_eq!(names("app.py", "gospy"), ["main"]);
    }

    #[test]
    fn u8_push_inputs_keep_every_profile_and_drop_annotations() {
        let sample = |id: &str| RawSample {
            raw_profile: fixture::pb(),
            id: id.to_string(),
        };
        let series = |svc: &str, samples: Vec<RawSample>| RawProfileSeries {
            labels: vec![LabelPair {
                name: "service_name".to_string(),
                value: svc.to_string(),
            }],
            samples,
            annotations: vec![ProfileAnnotation {
                key: "k".to_string(),
                value: "v".to_string(),
            }],
        };
        let req = PushRequest {
            series: vec![
                series("one", vec![sample("734fd599-6865-419e-9475-932762d8f469")]),
                series("two", vec![sample("abc"), sample("")]),
            ],
        };
        let inputs = push_inputs(req).expect("a valid push");
        assert_eq!(inputs.len(), 3);
        let services: Vec<&str> = inputs.iter().map(|i| i.labels[0].1.as_str()).collect();
        assert_eq!(services, ["one", "two", "two"]);
        assert!(inputs.iter().all(|i| i.annotations.is_empty()));
        assert_eq!(
            inputs[0].id,
            Some([
                0x73, 0x4f, 0xd5, 0x99, 0x68, 0x65, 0x41, 0x9e, 0x94, 0x75, 0x93, 0x27, 0x62, 0xd8,
                0xf4, 0x69
            ])
        );
        assert_eq!(inputs[1].id, None);
        assert_eq!(inputs[2].id, None);
        assert!(
            inputs
                .iter()
                .all(|i| i.decompressed_bytes == i.profile.encoded_len() as u64)
        );
    }
}
