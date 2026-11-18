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

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::io::Read as _;

use prost::Message;
use pulsus_profiles_wire::google_v1::{
    Function, Label, Line, Location, Mapping, Profile, Sample, ValueType,
};

use self::reader::{Correlation, Event, IdMap, Pools, Reader};
use crate::ingest::profiles::ProfileInput;
use crate::protocols::profile_model::{MAX_PROFILE_SIZE_BYTES, SPY_LABEL};
use crate::protocols::profile_push::{Labels, LegacyQuery, boundary_of, file_parts};

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

/// One kind of sample: its series name, sample types and period type
/// (`pprof/pprof.go:150-203`).
struct SampleKind {
    metric: &'static str,
    sample_types: &'static [(&'static str, &'static str)],
    period_type: Option<(&'static str, &'static str)>,
}

const CPU: usize = 0;
const WALL: usize = 1;
const IN_NEW_TLAB: usize = 2;
const OUTSIDE_TLAB: usize = 3;
const LOCK: usize = 4;
const PARK: usize = 5;
const LIVE_OBJECT: usize = 6;
const ALLOC_SAMPLE: usize = 7;
const MALLOC: usize = 8;

const COUNT_DELAY: &[(&str, &str)] = &[("contentions", "count"), ("delay", "nanoseconds")];

const KINDS: [SampleKind; 9] = [
    SampleKind {
        metric: "process_cpu",
        sample_types: &[("cpu", "nanoseconds")],
        period_type: Some(("cpu", "nanoseconds")),
    },
    SampleKind {
        metric: "wall",
        sample_types: &[("wall", "nanoseconds")],
        period_type: Some(("wall", "nanoseconds")),
    },
    SampleKind {
        metric: "memory",
        sample_types: &[
            ("alloc_in_new_tlab_objects", "count"),
            ("alloc_in_new_tlab_bytes", "bytes"),
        ],
        period_type: Some(("space", "bytes")),
    },
    SampleKind {
        metric: "memory",
        sample_types: &[
            ("alloc_outside_tlab_objects", "count"),
            ("alloc_outside_tlab_bytes", "bytes"),
        ],
        period_type: Some(("space", "bytes")),
    },
    SampleKind {
        metric: "mutex",
        sample_types: COUNT_DELAY,
        period_type: Some(("mutex", "count")),
    },
    SampleKind {
        metric: "block",
        sample_types: COUNT_DELAY,
        period_type: Some(("block", "count")),
    },
    SampleKind {
        metric: "memory",
        sample_types: &[("live", "count")],
        period_type: Some(("objects", "count")),
    },
    SampleKind {
        metric: "memory",
        sample_types: &[
            ("alloc_sample_objects", "count"),
            ("alloc_sample_bytes", "bytes"),
        ],
        period_type: Some(("space", "bytes")),
    },
    SampleKind {
        metric: "memory",
        sample_types: &[("malloc_objects", "count"), ("malloc_bytes", "bytes")],
        period_type: None,
    },
];

/// One profile being built (`pprof/profile_builder.go`): functions keyed by
/// method, locations by method and line, samples by stack and correlation.
/// Methods are keyed by their low 32 bits, as the library's are.
struct Builder {
    metric: &'static str,
    profile: Profile,
    strings: IdMap<String, i64>,
    functions: IdMap<u32, u64>,
    locations: IdMap<(u32, u32), u64>,
    samples: IdMap<(u64, Correlation), usize>,
}

impl Builder {
    fn new(kind: &SampleKind, time_nanos: i64, duration_nanos: i64) -> Builder {
        let mut b = Builder {
            metric: kind.metric,
            profile: Profile {
                time_nanos,
                duration_nanos,
                mapping: vec![Mapping {
                    id: 1,
                    has_functions: true,
                    ..Mapping::default()
                }],
                ..Profile::default()
            },
            strings: IdMap::default(),
            functions: IdMap::default(),
            locations: IdMap::default(),
            samples: IdMap::default(),
        };
        b.intern("");
        for (ty, unit) in kind.sample_types {
            let vt = ValueType {
                r#type: b.intern(ty),
                unit: b.intern(unit),
            };
            b.profile.sample_type.push(vt);
        }
        if let Some((ty, unit)) = kind.period_type {
            b.profile.period_type = Some(ValueType {
                r#type: b.intern(ty),
                unit: b.intern(unit),
            });
        }
        b
    }

    fn intern(&mut self, s: &str) -> i64 {
        if let Some(&i) = self.strings.get(s) {
            return i;
        }
        let i = self.profile.string_table.len() as i64;
        self.profile.string_table.push(s.to_string());
        self.strings.insert(s.to_string(), i);
        i
    }

    /// The sample's labels: its context's labels from the snapshot, then
    /// `profile_id`, `span_name` and `trace_id` from its correlation
    /// (`pprof/profile_builder.go:131-194`).
    fn sample_labels(&mut self, labels: &LabelsSnapshot, c: &Correlation) -> Vec<Label> {
        let text = |id: i64| labels.strings.get(&id).map_or("", String::as_str);
        let mut out = Vec::new();
        if let Some(ctx) = labels.contexts.get(&(c.context_id as i64)) {
            for (&k, &v) in &ctx.labels {
                let label = Label {
                    key: self.intern(text(k)),
                    str: self.intern(text(v)),
                    ..Label::default()
                };
                out.push(label);
            }
        }
        if c.span_id != 0 {
            let label = Label {
                key: self.intern("profile_id"),
                str: self.intern(&format!("{:016x}", c.span_id)),
                ..Label::default()
            };
            out.push(label);
        }
        if c.span_name != 0 {
            let name = text(c.span_name as i64);
            if !name.is_empty() {
                let label = Label {
                    key: self.intern("span_name"),
                    str: self.intern(name),
                    ..Label::default()
                };
                out.push(label);
            }
        }
        if c.trace_id_hi != 0 || c.trace_id_lo != 0 {
            let label = Label {
                key: self.intern("trace_id"),
                str: self.intern(&format!("{:016x}{:016x}", c.trace_id_hi, c.trace_id_lo)),
                ..Label::default()
            };
            out.push(label);
        }
        out
    }
}

/// A recording's profiles as its events arrive (`pprof/parser.go:52-150`,
/// `pprof/pprof.go:79-148`).
pub(crate) struct Builders {
    builders: [Option<Builder>; 9],
    time_nanos: i64,
    duration_nanos: i64,
    period: i64,
    event: String,
}

/// A lock or park duration in nanoseconds: its chunk's ticks scaled, or the
/// ticks when the chunk names no rate (entry 48).
fn delay_nanos(ticks: u64, ticks_per_second: u64) -> i64 {
    if ticks_per_second == 0 {
        return ticks as i64;
    }
    (i128::from(ticks) * 1_000_000_000 / i128::from(ticks_per_second)) as i64
}

impl Builders {
    pub(crate) fn new(time_nanos: i64, duration_nanos: i64, sample_rate: u32) -> Builders {
        Builders {
            builders: Default::default(),
            time_nanos,
            duration_nanos,
            period: if sample_rate == 0 {
                0
            } else {
                1_000_000_000 / i64::from(sample_rate)
            },
            event: String::new(),
        }
    }

    /// One event. Each counts once, a wall-clock sample as its `samples`
    /// (entry 49).
    pub(crate) fn add(
        &mut self,
        pools: &Pools<'_>,
        labels: &LabelsSnapshot,
        event: &Event<'_>,
        ticks_per_second: u64,
    ) {
        let state = |id: &u64| pools.thread_states.get(id).map(|s| s.as_ref());
        match event {
            Event::ExecutionSample {
                stack,
                state: st,
                correlation,
            } => {
                if state(st).is_some_and(|s| s != "STATE_SLEEPING") {
                    self.add_stack(CPU, pools, labels, *stack, correlation, &[1]);
                }
                if self.event == "wall" {
                    self.add_stack(WALL, pools, labels, *stack, correlation, &[1]);
                }
            }
            Event::WallClockSample {
                stack,
                state: st,
                samples,
                correlation,
            } => {
                let v = [i64::from(*samples)];
                if state(st) == Some("STATE_RUNNABLE") && self.event == "wall" {
                    self.add_stack(CPU, pools, labels, *stack, correlation, &v);
                }
                self.add_stack(WALL, pools, labels, *stack, correlation, &v);
            }
            Event::AllocInNewTlab {
                stack,
                tlab_size,
                correlation,
            } => {
                let v = [1, *tlab_size as i64];
                self.add_stack(IN_NEW_TLAB, pools, labels, *stack, correlation, &v);
            }
            Event::AllocOutsideTlab {
                stack,
                allocation_size,
                correlation,
            } => {
                let v = [1, *allocation_size as i64];
                self.add_stack(OUTSIDE_TLAB, pools, labels, *stack, correlation, &v);
            }
            Event::MonitorEnter {
                stack,
                duration,
                correlation,
            } => {
                let v = [1, delay_nanos(*duration, ticks_per_second)];
                self.add_stack(LOCK, pools, labels, *stack, correlation, &v);
            }
            Event::ThreadPark { stack, duration } => {
                let v = [1, delay_nanos(*duration, ticks_per_second)];
                self.add_stack(PARK, pools, labels, *stack, &Correlation::default(), &v);
            }
            Event::LiveObject { stack } => {
                let none = Correlation::default();
                self.add_stack(LIVE_OBJECT, pools, labels, *stack, &none, &[1]);
            }
            Event::AllocSample { stack, weight } => {
                let v = [1, *weight as i64];
                let none = Correlation::default();
                self.add_stack(ALLOC_SAMPLE, pools, labels, *stack, &none, &v);
            }
            Event::Malloc { stack, size } => {
                let v = [1, *size as i64];
                let none = Correlation::default();
                self.add_stack(MALLOC, pools, labels, *stack, &none, &v);
            }
            Event::ActiveSetting { name, value } => {
                if name == "event" {
                    self.event = value.to_string();
                }
            }
        }
    }

    /// Adds `values` to the sample of `stack` and `correlation`, making the
    /// builder first and the sample when it is new (`pprof/pprof.go:79-148`).
    fn add_stack(
        &mut self,
        kind: usize,
        pools: &Pools<'_>,
        labels: &LabelsSnapshot,
        stack: u64,
        correlation: &Correlation,
        values: &[i64],
    ) {
        let mul = if kind == CPU || kind == WALL {
            self.period
        } else {
            1
        };
        let (time_nanos, duration_nanos) = (self.time_nanos, self.duration_nanos);
        let b = self.builders[kind]
            .get_or_insert_with(|| Builder::new(&KINDS[kind], time_nanos, duration_nanos));
        let Some(frames) = pools.stack_traces.get(&stack) else {
            return;
        };
        if let Some(&i) = b.samples.get(&(stack, *correlation)) {
            for (d, v) in b.profile.sample[i].value.iter_mut().zip(values) {
                *d = d.wrapping_add(v.wrapping_mul(mul));
            }
            return;
        }
        let mut location_id = Vec::with_capacity(frames.len());
        for f in frames {
            let key = (f.method as u32, f.line);
            if let Some(&loc) = b.locations.get(&key) {
                location_id.push(loc);
                continue;
            }
            let Some(method) = pools.methods.get(&f.method) else {
                continue;
            };
            let function_id = match b.functions.get(&key.0) {
                Some(&id) => id,
                None => {
                    let Some(&class) = pools.classes.get(&method.class) else {
                        continue;
                    };
                    let name = format!("{}.{}", pools.symbol(class), pools.symbol(method.name));
                    let name = b.intern(&name);
                    let id = b.profile.function.len() as u64 + 1;
                    b.profile.function.push(Function {
                        id,
                        name,
                        ..Function::default()
                    });
                    b.functions.insert(key.0, id);
                    id
                }
            };
            let id = b.profile.location.len() as u64 + 1;
            b.profile.location.push(Location {
                id,
                mapping_id: 1,
                line: vec![Line {
                    function_id,
                    line: i64::from(f.line),
                }],
                ..Location::default()
            });
            b.locations.insert(key, id);
            location_id.push(id);
        }
        let value = values.iter().map(|v| v.wrapping_mul(mul)).collect();
        let label = b.sample_labels(labels, correlation);
        b.samples
            .insert((stack, *correlation), b.profile.sample.len());
        b.profile.sample.push(Sample {
            location_id,
            value,
            label,
        });
    }

    /// The profiles, by builder (§3.3), and the `event` setting.
    pub(crate) fn finish(self) -> JfrProfiles {
        JfrProfiles {
            event: self.event,
            profiles: self
                .builders
                .into_iter()
                .flatten()
                .map(|b| (b.metric, b.profile))
                .collect(),
        }
    }
}

/// The recording's profiles (§3.2 to §3.4): `time_nanos` and
/// `duration_nanos` are every profile's, and a `sample_rate` other than 0
/// scales CPU and wall samples by its period.
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

/// The reference's `decompress` (`profile.go:136-162`): a gzip body under
/// the profile size limit, anything else as it is.
fn decompress(bytes: &[u8]) -> Result<Cow<'_, [u8]>, String> {
    if bytes.len() < 2 {
        return Err("failed to read magic".to_string());
    }
    if bytes[..2] != [0x1f, 0x8b] {
        return Ok(Cow::Borrowed(bytes));
    }
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .take(MAX_PROFILE_SIZE_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("failed to decompress jfr: {e}"))?;
    if out.len() as u64 > MAX_PROFILE_SIZE_BYTES {
        return Err(format!(
            "decompressed size exceeds maximum allowed size of {MAX_PROFILE_SIZE_BYTES} bytes"
        ));
    }
    Ok(Cow::Owned(out))
}

/// The form's `jfr` and `labels` parts (`profile.go:93-134`).
fn read_form<'b>(
    content_type: &str,
    body: &'b [u8],
) -> Result<(Cow<'b, [u8]>, LabelsSnapshot), String> {
    let boundary = boundary_of(content_type).ok_or("malformed multipart content type header")?;
    let parts = file_parts(body, &boundary)?;
    let field = |name: &str| {
        parts
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| *b)
            .filter(|b| !b.is_empty())
    };
    let jfr = field("jfr").ok_or("jfr field is required")?;
    let jfr =
        decompress(jfr).map_err(|e| format!("loadJFRFromForm failed to decompress jfr: {e}"))?;
    let mut labels = LabelsSnapshot::default();
    if let Some(part) = field("labels") {
        let part = decompress(part)
            .map_err(|e| format!("loadJFRFromForm failed to decompress labels: {e}"))?;
        labels = LabelsSnapshot::decode(part.as_ref())
            .map_err(|e| format!("failed to parse labels form field: {e}"))?;
    }
    Ok((jfr, labels))
}

/// A JFR push as the model's inputs (§3.1, §3.5): one per profile, each
/// under the reference's series labels. An error is the text the answer
/// carries after `parsing IngestInput-pprof failed `.
pub fn jfr_inputs(
    query: &LegacyQuery,
    content_type: &str,
    body: &[u8],
    now_ns: i64,
) -> Result<Vec<ProfileInput>, String> {
    let (jfr, labels) = if content_type.contains("multipart/form-data") {
        read_form(content_type, body)?
    } else {
        (Cow::Borrowed(body), LabelsSnapshot::default())
    };
    let time_nanos = query.from_ns.unwrap_or(now_ns);
    let duration_nanos = query.until_ns.unwrap_or(now_ns).wrapping_sub(time_nanos);
    let out = parse_jfr(&jfr, &labels, time_nanos, duration_nanos, query.sample_rate)?;

    let service = query
        .name
        .iter()
        .find(|(k, _)| k == "__name__")
        .map_or("", |(_, v)| v.as_str());
    let kept: Labels = query
        .name
        .iter()
        .filter(|(k, _)| !k.starts_with("__") || k == "__session_id__")
        .cloned()
        .collect();
    let service_label = if kept.iter().any(|(k, _)| k == "service_name") {
        "app_name"
    } else {
        "service_name"
    };
    let add_service = !service.is_empty() && !kept.iter().any(|(k, _)| k == service_label);
    Ok(out
        .profiles
        .into_iter()
        .map(|(metric, profile)| {
            let mut labels = kept.clone();
            labels.push((SPY_LABEL.to_string(), query.spy_name.clone()));
            labels.push(("__delta__".to_string(), "false".to_string()));
            labels.push(("jfr_event".to_string(), out.event.clone()));
            labels.push(("__name__".to_string(), metric.to_string()));
            if add_service {
                labels.push((service_label.to_string(), service.to_string()));
            }
            ProfileInput {
                labels,
                decompressed_bytes: profile.encoded_len() as u64,
                profile,
                id: None,
                annotations: Vec::new(),
                language: None,
            }
        })
        .collect())
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
