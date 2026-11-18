//! JFR recordings on `/ingest` (`format=jfr`, issue #819): the body, or the
//! form's `jfr` and `labels` parts, becomes one profile per kind of sample it
//! holds, each the model's input under the reference's series labels. A close
//! port of the JFR library the reference pins, at tag `v0.19.0`
//! (`pprof/parser.go`, `pprof.go`, `profile_builder.go` and
//! `pprof/<reference>/<reference>.go`), which is Apache-2.0, and of the
//! reference's `pkg/og/convert/jfr/profile.go:31-162`. Two of the library's
//! results are corrected (`docs/reference-defects-we-do-not-copy.md`
//! entries 48 and 49). Pure functions, no I/O.

mod reader;

#[cfg(test)]
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use prost::Message;
use pulsus_profiles_wire::google_v1::Profile;
#[cfg(test)]
use pulsus_profiles_wire::google_v1::{Sample, ValueType};

#[cfg(test)]
use self::reader::Correlation;
use self::reader::{Event, Pools, Reader};
use crate::ingest::profiles::ProfileInput;
#[cfg(test)]
use crate::protocols::profile_model::SPY_LABEL;
use crate::protocols::profile_push::LegacyQuery;

/// The `labels` form part: each context id's labels, as ids into `strings`
/// (`pprof/jfr_labels.proto`).
#[derive(Clone, PartialEq, Message)]
pub struct LabelsSnapshot {
    #[prost(map = "int64, message", tag = "1")]
    pub contexts: HashMap<i64, Context>,
    #[prost(map = "int64, string", tag = "2")]
    pub strings: HashMap<i64, String>,
}

/// One context's labels: key id to value id.
#[derive(Clone, PartialEq, Message)]
pub struct Context {
    #[prost(btree_map = "int64, int64", tag = "1")]
    pub labels: BTreeMap<i64, i64>,
}

/// A recording's profiles, by kind of sample, and its `event` setting.
#[derive(Debug, Clone, PartialEq)]
pub struct JfrProfiles {
    /// The last `jdk.ActiveSetting` named `event`.
    pub event: String,
    /// Each profile and its series name, in the order of §3.3's builders.
    pub profiles: Vec<(&'static str, Profile)>,
}

/// Stub until the change: builds nothing.
pub(crate) struct Builders {}

impl Builders {
    pub(crate) fn new(_time_nanos: i64, _duration_nanos: i64, _sample_rate: u32) -> Builders {
        Builders {}
    }

    pub(crate) fn add(
        &mut self,
        _pools: &Pools<'_>,
        _labels: &LabelsSnapshot,
        _event: &Event<'_>,
        _ticks_per_second: u64,
    ) {
    }

    pub(crate) fn finish(self) -> JfrProfiles {
        JfrProfiles {
            event: String::new(),
            profiles: Vec::new(),
        }
    }
}

/// The recording's profiles.
pub fn parse_jfr(
    body: &[u8],
    labels: &LabelsSnapshot,
    time_nanos: i64,
    duration_nanos: i64,
    sample_rate: u32,
) -> Result<JfrProfiles, String> {
    let mut reader = Reader::new(body);
    let mut builders = Builders::new(time_nanos, duration_nanos, sample_rate);
    while let Some(event) = reader.next_event()? {
        builders.add(reader.pools(), labels, &event, reader.ticks_per_second());
    }
    Ok(builders.finish())
}

/// Stub until the change: no input.
pub fn jfr_inputs(
    _query: &LegacyQuery,
    _content_type: &str,
    _body: &[u8],
    _now_ns: i64,
) -> Result<Vec<ProfileInput>, String> {
    Ok(Vec::new())
}

#[cfg(test)]
pub(crate) mod test_support {
    //! The vendored recordings and the library's own rendering of a profile,
    //! shared with the route tests in `ingest/profiles_http.rs`.

    use std::io::Read as _;

    use pulsus_profiles_wire::google_v1::Profile;

    /// `from` and `until` of the library's own tests
    /// (`pprof/parser_test.go:302-306`).
    pub(crate) const FROM_NS: i64 = 1_706_241_880_000_000_000;
    pub(crate) const DURATION_NS: i64 = 10_000_000_000;

    /// A vendored file's bytes, as stored.
    pub(crate) fn fixture(name: &str) -> Vec<u8> {
        let path = format!(
            "{}/tests/fixtures/profileql_jfr/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    pub(crate) fn gunzip(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        flate2::read::MultiGzDecoder::new(bytes)
            .read_to_end(&mut out)
            .expect("a gzip file");
        out
    }

    /// A vendored file, unzipped.
    pub(crate) fn unzipped(name: &str) -> Vec<u8> {
        gunzip(&fixture(name))
    }

    /// A vendored text file, unzipped.
    pub(crate) fn expected(name: &str) -> String {
        String::from_utf8(unzipped(name)).expect("UTF-8")
    }

    /// The library's `stackCollapseProto` with line numbers
    /// (`pprof/parser_test.go:495-572`): per sample its frames root first as
    /// `name:line`, joined by `;`, then its values; sorted, all-zero samples
    /// dropped and equal stacks summed.
    pub(crate) fn collapsed(p: &Profile) -> String {
        let name = |loc: u64| -> Vec<String> {
            let l = p.location.iter().find(|l| l.id == loc).expect("location");
            l.line
                .iter()
                .map(|line| {
                    let f = p
                        .function
                        .iter()
                        .find(|f| f.id == line.function_id)
                        .expect("function");
                    format!("{}:{}", p.string_table[f.name as usize], line.line)
                })
                .collect()
        };
        let mut stacks: Vec<(String, Vec<i64>)> = p
            .sample
            .iter()
            .map(|s| {
                let funcs: Vec<String> =
                    s.location_id.iter().rev().flat_map(|&l| name(l)).collect();
                (funcs.join(";"), s.value.clone())
            })
            .collect();
        stacks.sort_by(|a, b| a.0.cmp(&b.0));
        let mut unique: Vec<(String, Vec<i64>)> = Vec::new();
        for (funcs, value) in stacks {
            if value.iter().all(|&v| v == 0) {
                continue;
            }
            match unique.last_mut() {
                Some(last) if last.0 == funcs => {
                    for (a, b) in last.1.iter_mut().zip(&value) {
                        *a += b;
                    }
                }
                _ => unique.push((funcs, value)),
            }
        }
        unique
            .iter()
            .map(|(funcs, value)| {
                let v: Vec<String> = value.iter().map(i64::to_string).collect();
                format!("{funcs} [{}]", v.join(" "))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Collapsed text as stack to values.
    pub(crate) fn collapsed_lines(text: &str) -> Vec<(String, Vec<i64>)> {
        text.lines()
            .map(|l| {
                let (funcs, values) = l.rsplit_once(" [").expect("a collapsed line");
                let values = values
                    .trim_end_matches(']')
                    .split(' ')
                    .map(|v| v.parse().expect("a value"))
                    .collect();
                (funcs.to_string(), values)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::reader::{Frame, Method};
    use super::test_support::*;
    use super::*;
    use crate::protocols::profile_push::fixture::{gzip, multipart};
    use crate::protocols::profile_push::parse_name;

    fn parse(name: &str, labels: &LabelsSnapshot) -> Result<JfrProfiles, String> {
        parse_jfr(&unzipped(name), labels, FROM_NS, DURATION_NS, 100)
    }

    fn types(p: &Profile) -> (Vec<String>, Option<String>) {
        let s = |vt: &ValueType| {
            format!(
                "{}/{}",
                p.string_table[vt.r#type as usize], p.string_table[vt.unit as usize]
            )
        };
        (
            p.sample_type.iter().map(s).collect(),
            p.period_type.as_ref().map(s),
        )
    }

    fn kinds(out: &JfrProfiles) -> Vec<(String, Vec<String>, Option<String>)> {
        out.profiles
            .iter()
            .map(|(m, p)| {
                let (st, pt) = types(p);
                (m.to_string(), st, pt)
            })
            .collect()
    }

    fn kind(metric: &str, st: &[&str], pt: Option<&str>) -> (String, Vec<String>, Option<String>) {
        (
            metric.to_string(),
            st.iter().map(|s| s.to_string()).collect(),
            pt.map(str::to_string),
        )
    }

    /// The library's ticks per second in `example.jfr`'s one chunk.
    const EXAMPLE_TICKS: i128 = 2_303_974_730;

    #[test]
    fn j1_example_becomes_the_references_four_profiles() {
        let out = parse("example.jfr.gz", &LabelsSnapshot::default()).expect("parses");
        assert_eq!(
            kinds(&out),
            [
                kind("process_cpu", &["cpu/nanoseconds"], Some("cpu/nanoseconds")),
                kind(
                    "memory",
                    &[
                        "alloc_in_new_tlab_objects/count",
                        "alloc_in_new_tlab_bytes/bytes"
                    ],
                    Some("space/bytes")
                ),
                kind(
                    "memory",
                    &[
                        "alloc_outside_tlab_objects/count",
                        "alloc_outside_tlab_bytes/bytes"
                    ],
                    Some("space/bytes")
                ),
                kind(
                    "mutex",
                    &["contentions/count", "delay/nanoseconds"],
                    Some("mutex/count")
                ),
            ]
        );
        for (m, p) in &out.profiles {
            assert_eq!(
                (p.time_nanos, p.duration_nanos, p.period),
                (FROM_NS, DURATION_NS, 0),
                "{m}"
            );
        }
        for (i, file) in [
            (0, "example.cpu.txt.gz"),
            (1, "example.alloc_in.txt.gz"),
            (2, "example.alloc_out.txt.gz"),
        ] {
            assert_eq!(collapsed(&out.profiles[i].1), expected(file), "{file}");
        }

        // The mutex profile: each stack's count is the library's; its delay
        // is the library's tick total in nanoseconds, each event truncated.
        let ours = collapsed_lines(&collapsed(&out.profiles[3].1));
        let theirs = collapsed_lines(&expected("example.mutex.txt.gz"));
        let stacks =
            |v: &[(String, Vec<i64>)]| v.iter().map(|(s, _)| s.clone()).collect::<Vec<_>>();
        assert_eq!(stacks(&ours), stacks(&theirs));
        for ((stack, ours), (_, theirs)) in ours.iter().zip(&theirs) {
            let (c, f) = (theirs[0], theirs[1]);
            let whole = (i128::from(f) * 1_000_000_000 / EXAMPLE_TICKS) as i64;
            assert_eq!(ours[0], c, "{stack}");
            assert!(
                whole - c < ours[1] && ours[1] <= whole,
                "{stack}: {} ns for {f} ticks",
                ours[1]
            );
        }
    }

    #[test]
    fn j2_the_labels_snapshot_labels_the_samples() {
        let labels =
            LabelsSnapshot::decode(unzipped("dump1.labels.pb.gz").as_slice()).expect("labels");
        let out = parse("dump1.jfr.gz", &labels).expect("parses");
        assert_eq!(out.profiles.len(), 1);
        let p = &out.profiles[0].1;
        assert_eq!(collapsed(p), expected("dump1.cpu.txt.gz"));
        let thread = |s: &Sample| {
            s.label
                .iter()
                .find(|l| p.string_table[l.key as usize] == "thread_name")
                .map(|l| p.string_table[l.str as usize].clone())
        };
        let names: BTreeSet<String> = p.sample.iter().filter_map(thread).collect();
        let want: BTreeSet<String> = [1, 3, 5, 7]
            .iter()
            .map(|n| format!("pool-2-thread-{n}"))
            .collect();
        assert_eq!(names, want);
        assert!(p.sample.iter().any(|s| thread(s).is_none()));
    }

    #[test]
    fn j3_a_wall_recording_takes_cpu_from_its_runnable_samples() {
        let out = parse("wall.jfr.gz", &LabelsSnapshot::default()).expect("parses");
        assert_eq!(out.event, "wall");
        let metrics: Vec<&str> = out.profiles.iter().map(|(m, _)| *m).collect();
        assert_eq!(metrics, ["process_cpu", "wall"]);
        assert_eq!(collapsed(&out.profiles[0].1), expected("wall.cpu.txt.gz"));
        assert_eq!(collapsed(&out.profiles[1].1), expected("wall.wall.txt.gz"));
    }

    /// Pools holding one stack trace, id 1: `A.m` at line 10.
    fn one_stack() -> Pools<'static> {
        let mut p = Pools::default();
        p.stack_traces.insert(
            1,
            vec![Frame {
                method: 1,
                line: 10,
            }],
        );
        p.methods.insert(1, Method { class: 1, name: 2 });
        p.classes.insert(1, 1);
        p.symbols.insert(1, Cow::Borrowed("A"));
        p.symbols.insert(2, Cow::Borrowed("m"));
        p
    }

    /// Every sample's values of the profiles named `metric`.
    fn values(out: &JfrProfiles, metric: &str) -> Vec<Vec<i64>> {
        out.profiles
            .iter()
            .filter(|(m, _)| *m == metric)
            .flat_map(|(_, p)| p.sample.iter().map(|s| s.value.clone()))
            .collect()
    }

    #[test]
    fn j5_lock_and_park_delays_are_nanoseconds() {
        let pools = one_stack();
        let labels = LabelsSnapshot::default();
        let mut b = Builders::new(FROM_NS, DURATION_NS, 100);
        let ticks = 2_000_000_000;
        b.add(
            &pools,
            &labels,
            &Event::MonitorEnter {
                stack: 1,
                duration: 3_000,
                correlation: Correlation::default(),
            },
            ticks,
        );
        b.add(
            &pools,
            &labels,
            &Event::ThreadPark {
                stack: 1,
                duration: 4_000,
            },
            ticks,
        );
        let out = b.finish();
        assert_eq!(values(&out, "mutex"), [vec![1, 1_500]]);
        assert_eq!(values(&out, "block"), [vec![1, 2_000]]);
    }

    #[test]
    fn j6_an_allocation_counts_once_after_a_wall_sample() {
        let pools = one_stack();
        let labels = LabelsSnapshot::default();
        let mut b = Builders::new(FROM_NS, DURATION_NS, 100);
        b.add(
            &pools,
            &labels,
            &Event::WallClockSample {
                stack: 1,
                state: 0,
                samples: 5,
                correlation: Correlation::default(),
            },
            1_000_000_000,
        );
        b.add(
            &pools,
            &labels,
            &Event::AllocInNewTlab {
                stack: 1,
                tlab_size: 100,
                correlation: Correlation::default(),
            },
            1_000_000_000,
        );
        let out = b.finish();
        assert_eq!(values(&out, "memory"), [vec![1, 100]]);
    }

    fn query(name: &str, spy: &str) -> LegacyQuery {
        LegacyQuery {
            name: parse_name(name).expect("a valid name"),
            from_ns: Some(FROM_NS),
            until_ns: Some(FROM_NS + DURATION_NS),
            spy_name: spy.to_string(),
            format: Some("jfr".to_string()),
            sample_rate: 100,
        }
    }

    fn pairs(ls: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = ls
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn j8_the_series_labels_are_the_references() {
        let body = unzipped("example.jfr.gz");
        let cpu_labels = |name: &str| {
            let inputs = jfr_inputs(&query(name, "javaspy"), "", &body, 0).expect("parses");
            inputs
                .iter()
                .find(|i| {
                    i.labels
                        .iter()
                        .any(|l| *l == ("__name__".into(), "process_cpu".into()))
                })
                .map(|i| {
                    let mut ls = i.labels.clone();
                    ls.sort();
                    ls
                })
        };
        let with = |more: &[(&str, &str)]| {
            let mut all = vec![
                (SPY_LABEL, "javaspy"),
                ("__delta__", "false"),
                ("jfr_event", "cpu"),
                ("__name__", "process_cpu"),
            ];
            all.extend_from_slice(more);
            Some(pairs(&all))
        };
        assert_eq!(
            cpu_labels("checkout{env=prod,__x__=y,__session_id__=s}"),
            with(&[
                ("env", "prod"),
                ("__session_id__", "s"),
                ("service_name", "checkout")
            ])
        );
        assert_eq!(
            cpu_labels("checkout{service_name=svc}"),
            with(&[("service_name", "svc"), ("app_name", "checkout")])
        );
        assert_eq!(cpu_labels("app.cpu"), with(&[("service_name", "app.cpu")]));
    }

    #[test]
    fn j9_the_form_carries_the_recording_gzipped() {
        let q = query("checkout", "javaspy");
        let ct = "multipart/form-data; boundary=b0undary";
        let counts = |inputs: &[ProfileInput]| {
            inputs
                .iter()
                .map(|i| i.profile.sample.len())
                .collect::<Vec<_>>()
        };
        let raw = parse("example.jfr.gz", &LabelsSnapshot::default()).expect("parses");
        let raw: Vec<usize> = raw.profiles.iter().map(|(_, p)| p.sample.len()).collect();
        assert_eq!(raw.len(), 4);
        let body = multipart(
            "b0undary",
            &[("jfr", Some("jfr"), &fixture("example.jfr.gz"))],
        );
        let form = jfr_inputs(&q, ct, &body, 0).map(|i| counts(&i));
        assert_eq!(form, Ok(raw));

        let body = multipart("b0undary", &[("other", Some("x"), b"x")]);
        assert_eq!(
            jfr_inputs(&q, ct, &body, 0).map(|i| i.len()),
            Err("jfr field is required".to_string())
        );
        let big = gzip(&vec![0u8; 4_194_305]);
        let body = multipart("b0undary", &[("jfr", Some("jfr"), &big)]);
        assert_eq!(
            jfr_inputs(&q, ct, &body, 0).map(|i| i.len()),
            Err(
                "loadJFRFromForm failed to decompress jfr: decompressed size exceeds maximum \
                 allowed size of 4194304 bytes"
                    .to_string()
            )
        );
    }
}
