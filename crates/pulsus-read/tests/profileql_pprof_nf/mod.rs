//! Issue #828 (plan §8.1): fixture P and `NF`, the decoded form every pprof
//! export case compares.
//!
//! Included by the pprof suites through `#[path]`, so cargo never builds it
//! as its own test binary: `pulsus-read`'s unit tests, its live and cluster
//! suites, and `pulsus-server`'s route suite and differential.
//!
//! `NF` is the header and the multiset of samples, each sample a path of
//! decoded locations, leaf first. Ids and string order are not part of it
//! (plan D4). A location whose one line is the function `other` with no
//! file is read with no mapping: the reference gives it whichever mapping
//! its table holds first.

#![allow(dead_code)]

use std::collections::BTreeMap;

use pulsus_profiles_wire::google_v1::{self, Profile};

/// One value count of fixture P, in `cpu` nanoseconds.
pub const UNIT: i64 = 10_000_000;
/// P's pushed period.
pub const PERIOD: i64 = 10_000_000;
/// The type id P is stored under.
pub const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
/// The trace id P's first sample carries (T1).
pub const T1: &str = "0102030405060708090a0b0c0d0e0f10";

/// A mapping: file, build id, start, limit, offset, and the four flags as
/// bits 0–3.
pub type NfMapping = (String, String, u64, u64, u64, u8);
/// A function: name, system name, file, start line.
pub type NfFunction = (String, String, String, i64);

/// One decoded location.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NfLocation {
    pub mapping: Option<NfMapping>,
    pub address: u64,
    pub is_folded: bool,
    pub lines: Vec<(NfFunction, i64)>,
}

/// A profile's decoded form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nf {
    pub sample_type: Vec<(String, String)>,
    pub period_type: Option<(String, String)>,
    pub default_sample_type: String,
    pub period: i64,
    pub time_nanos: i64,
    pub duration_nanos: i64,
    /// `(path, values)` → how many samples carry it.
    pub samples: BTreeMap<(Vec<NfLocation>, Vec<i64>), usize>,
}

impl Nf {
    /// The same form with `period` set aside.
    pub fn without_period(&self) -> Nf {
        Nf {
            period: 0,
            ..self.clone()
        }
    }
}

/// `p` decoded.
pub fn nf(p: &Profile) -> Nf {
    let s = |i: i64| -> String {
        usize::try_from(i)
            .ok()
            .and_then(|i| p.string_table.get(i))
            .cloned()
            .unwrap_or_default()
    };
    let vt = |v: &google_v1::ValueType| (s(v.r#type), s(v.unit));
    let mappings: BTreeMap<u64, &google_v1::Mapping> =
        p.mapping.iter().map(|m| (m.id, m)).collect();
    let functions: BTreeMap<u64, &google_v1::Function> =
        p.function.iter().map(|f| (f.id, f)).collect();
    let locations: BTreeMap<u64, &google_v1::Location> =
        p.location.iter().map(|l| (l.id, l)).collect();
    let location = |id: u64| -> NfLocation {
        let Some(l) = locations.get(&id) else {
            return NfLocation {
                mapping: None,
                address: 0,
                is_folded: false,
                lines: vec![(
                    (
                        format!("<no location {id}>"),
                        String::new(),
                        String::new(),
                        0,
                    ),
                    0,
                )],
            };
        };
        let lines: Vec<(NfFunction, i64)> = l
            .line
            .iter()
            .map(|line| {
                let f = functions.get(&line.function_id);
                let key = f.map_or_else(
                    || (String::new(), String::new(), String::new(), 0),
                    |f| (s(f.name), s(f.system_name), s(f.filename), f.start_line),
                );
                (key, line.line)
            })
            .collect();
        let stub = lines.len() == 1 && lines[0].0.0 == "other" && lines[0].0.2.is_empty();
        let mapping = if stub {
            None
        } else {
            mappings.get(&l.mapping_id).map(|m| {
                let flags = u8::from(m.has_functions)
                    | u8::from(m.has_filenames) << 1
                    | u8::from(m.has_line_numbers) << 2
                    | u8::from(m.has_inline_frames) << 3;
                (
                    s(m.filename),
                    s(m.build_id),
                    m.memory_start,
                    m.memory_limit,
                    m.file_offset,
                    flags,
                )
            })
        };
        NfLocation {
            mapping,
            address: l.address,
            is_folded: l.is_folded,
            lines,
        }
    };
    let mut samples = BTreeMap::new();
    for sample in &p.sample {
        let path: Vec<NfLocation> = sample.location_id.iter().map(|&id| location(id)).collect();
        *samples.entry((path, sample.value.clone())).or_insert(0) += 1;
    }
    Nf {
        sample_type: p.sample_type.iter().map(vt).collect(),
        period_type: p.period_type.as_ref().map(vt),
        default_sample_type: s(p.default_sample_type),
        period: p.period,
        time_nanos: p.time_nanos,
        duration_nanos: p.duration_nanos,
        samples,
    }
}

// ---------------------------------------------------------------------
// Fixture P
// ---------------------------------------------------------------------

/// P's functions and their start lines; file `main.go`, system name = name.
pub const FUNCTIONS: [(&str, i64); 7] = [
    ("main", 1),
    ("a", 2),
    ("b", 3),
    ("c", 4),
    ("d", 5),
    ("e", 6),
    ("zz", 7),
];

/// L1–L8's one line each, `(function, line)`.
pub const LINES: [(&str, i64); 8] = [
    ("main", 10),
    ("a", 20),
    ("a", 21),
    ("b", 30),
    ("c", 40),
    ("d", 50),
    ("e", 60),
    ("zz", 70),
];

/// L9's address, in mapping 2.
pub const L9_ADDRESS: u64 = 0x7f_0100;
/// Mapping 2's bounds.
pub const LIBX_START: u64 = 0x7f_0000;
pub const LIBX_LIMIT: u64 = 0x7f_ffff;

/// P's samples, leaf first, as indices 1–9 of its locations, and counts.
pub const SAMPLES: [(&[u64], i64); 7] = [
    (&[2, 1], 2),
    (&[3, 1], 3),
    (&[4, 2, 1], 10),
    (&[5, 3, 1], 1),
    (&[7, 6, 1], 4),
    (&[8], 1),
    (&[9], 6),
];

/// A function of P (or any other name: start line 0).
pub fn function(name: &str) -> NfFunction {
    let start = FUNCTIONS
        .iter()
        .find(|(n, _)| *n == name)
        .map_or(0, |(_, s)| *s);
    (
        name.to_string(),
        name.to_string(),
        "main.go".to_string(),
        start,
    )
}

/// Mapping 1 as stored: `has_functions` clears its bounds.
pub fn mapping1() -> NfMapping {
    ("app".to_string(), "bid".to_string(), 0, 0, 0, 0b1111)
}

/// Mapping 2 as stored: its bounds kept, no flags.
pub fn mapping2() -> NfMapping {
    (
        "libx.so".to_string(),
        String::new(),
        LIBX_START,
        LIBX_LIMIT,
        0,
        0,
    )
}

/// The cut's stub location.
pub fn stub() -> NfLocation {
    NfLocation {
        mapping: None,
        address: 0,
        is_folded: false,
        lines: vec![(
            ("other".to_string(), "other".to_string(), String::new(), 0),
            0,
        )],
    }
}

/// One location of a path as §3.6 writes it: `f:n` is the location of
/// mapping 1 whose one line is `f` at line `n`, `L9` is L9, `other` is the
/// stub.
pub fn location(token: &str) -> NfLocation {
    match token {
        "other" => stub(),
        "L9" => NfLocation {
            mapping: Some(mapping2()),
            address: L9_ADDRESS,
            is_folded: false,
            lines: Vec::new(),
        },
        _ => {
            let (name, line) = token.split_once(':').expect("f:n");
            NfLocation {
                mapping: Some(mapping1()),
                address: 0,
                is_folded: false,
                lines: vec![(function(name), line.parse().expect("a line"))],
            }
        }
    }
}

/// A path as §3.6 writes it, leaf first.
pub fn path(text: &str) -> Vec<NfLocation> {
    text.split_whitespace().map(location).collect()
}

/// The header §3.4 writes for P's type.
pub fn header(period: i64, time_nanos: i64) -> Nf {
    Nf {
        sample_type: vec![("cpu".to_string(), "nanoseconds".to_string())],
        period_type: Some(("cpu".to_string(), "nanoseconds".to_string())),
        default_sample_type: "cpu".to_string(),
        period,
        time_nanos,
        duration_nanos: 0,
        samples: BTreeMap::new(),
    }
}

/// `NF` of §3.6's samples, each `(path, count)`, values `count × unit`.
pub fn expected(samples: &[(&str, i64)], unit: i64, period: i64, time_nanos: i64) -> Nf {
    let mut out = header(period, time_nanos);
    for (p, count) in samples {
        *out.samples
            .entry((path(p), vec![count * unit]))
            .or_insert(0) += 1;
    }
    out
}

/// §3.6 P1.
pub const P1: [(&str, i64); 7] = [
    ("a:20 main:10", 2),
    ("a:21 main:10", 3),
    ("b:30 a:20 main:10", 10),
    ("c:40 a:21 main:10", 1),
    ("e:60 d:50 main:10", 4),
    ("zz:70", 1),
    ("L9", 6),
];
/// §3.6 P2.
pub const P2: [(&str, i64); 7] = [
    ("a:20 main:10", 2),
    ("a:21 main:10", 3),
    ("b:30 a:20 main:10", 10),
    ("other a:21 main:10", 1),
    ("other main:10", 4),
    ("other", 1),
    ("L9", 6),
];
/// §3.6 P2-ref.
pub const P2_REF: [(&str, i64); 6] = [
    ("a:20 main:10", 2),
    ("a:21 main:10", 3),
    ("b:30 a:20 main:10", 10),
    ("c:40 a:21 main:10", 1),
    ("other d:50 main:10", 4),
    ("other", 7),
];
/// §3.6 P3-ref.
pub const P3_REF: [(&str, i64); 7] = [
    ("a:20 main:10", 2),
    ("a:21 main:10", 3),
    ("b:30 a:20 main:10", 10),
    ("c:40 a:21 main:10", 1),
    ("e:60 d:50 main:10", 4),
    ("zz:70", 1),
    ("other", 6),
];
/// §3.6 P4.
pub const P4: [(&str, i64); 4] = [
    ("a:20 main:10", 2),
    ("a:21 main:10", 3),
    ("b:30 a:20 main:10", 10),
    ("c:40 a:21 main:10", 1),
];
/// §3.6 P5.
pub const P5: [(&str, i64); 6] = [
    ("a:0 main:10", 5),
    ("b:0 a:20 main:10", 10),
    ("c:0 a:21 main:10", 1),
    ("e:0 d:50 main:10", 4),
    ("zz:0", 1),
    ("L9", 6),
];
/// §3.6 P6.
pub const P6: [(&str, i64); 7] = [
    ("a:20", 2),
    ("a:21", 3),
    ("b:30", 10),
    ("c:40", 1),
    ("e:60", 4),
    ("zz:70", 1),
    ("L9", 6),
];
/// §3.6 P7.
pub const P7: [(&str, i64); 1] = [("a:20 main:10", 2)];

/// P as a pprof profile: every value `count × 10,000,000 × scale`, the
/// first sample labelled `trace_id` T1.
pub fn profile(scale: i64, period: i64, time_nanos: i64) -> Profile {
    let mut strings: Vec<String> = vec![String::new()];
    let mut s = |v: &str| -> i64 {
        if let Some(i) = strings.iter().position(|x| x == v) {
            return i as i64;
        }
        strings.push(v.to_string());
        (strings.len() - 1) as i64
    };
    let cpu = s("cpu");
    let ns = s("nanoseconds");
    let main_go = s("main.go");
    let function: Vec<google_v1::Function> = FUNCTIONS
        .iter()
        .enumerate()
        .map(|(i, (name, start))| {
            let n = s(name);
            google_v1::Function {
                id: i as u64 + 1,
                name: n,
                system_name: n,
                filename: main_go,
                start_line: *start,
            }
        })
        .collect();
    let mapping = vec![
        google_v1::Mapping {
            id: 1,
            memory_start: 0x40_0000,
            memory_limit: 0x80_0000,
            file_offset: 0,
            filename: s("app"),
            build_id: s("bid"),
            has_functions: true,
            has_filenames: true,
            has_line_numbers: true,
            has_inline_frames: true,
        },
        google_v1::Mapping {
            id: 2,
            memory_start: LIBX_START,
            memory_limit: LIBX_LIMIT,
            file_offset: 0,
            filename: s("libx.so"),
            ..google_v1::Mapping::default()
        },
    ];
    let mut location: Vec<google_v1::Location> = LINES
        .iter()
        .enumerate()
        .map(|(i, (name, line))| {
            let fid = FUNCTIONS
                .iter()
                .position(|(n, _)| n == name)
                .expect("a function") as u64
                + 1;
            google_v1::Location {
                id: i as u64 + 1,
                mapping_id: 1,
                address: 0x40_0110 + 0x10 * i as u64,
                line: vec![google_v1::Line {
                    function_id: fid,
                    line: *line,
                }],
                is_folded: false,
            }
        })
        .collect();
    location.push(google_v1::Location {
        id: 9,
        mapping_id: 2,
        address: L9_ADDRESS,
        line: Vec::new(),
        is_folded: false,
    });
    let trace_key = s("trace_id");
    let trace_value = s(T1);
    let sample = SAMPLES
        .iter()
        .enumerate()
        .map(|(i, (locs, count))| google_v1::Sample {
            location_id: locs.to_vec(),
            value: vec![count * UNIT * scale],
            label: if i == 0 {
                vec![google_v1::Label {
                    key: trace_key,
                    str: trace_value,
                    ..google_v1::Label::default()
                }]
            } else {
                Vec::new()
            },
        })
        .collect();
    Profile {
        sample_type: vec![google_v1::ValueType {
            r#type: cpu,
            unit: ns,
        }],
        sample,
        mapping,
        location,
        function,
        string_table: strings,
        time_nanos,
        duration_nanos: 10_000_000_000,
        period_type: Some(google_v1::ValueType {
            r#type: cpu,
            unit: ns,
        }),
        period,
        ..Profile::default()
    }
}
