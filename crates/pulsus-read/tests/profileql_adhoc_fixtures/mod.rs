//! Issue #831: the ad hoc upload fixtures of plan §7, as the files the
//! exploration app uploads. Shared by the unit tests, the live, cluster
//! and differential suites, each including this file by path.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::Write as _;

use prost::Message as _;
use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};

/// A pprof profile: `types` as `(type, unit)`, the period type, and each
/// sample's frames root first with one value per type. A frame `0x…` is a
/// location with that address and no lines; every other frame is one
/// function, its location's id and its function's id its first position.
pub fn pprof(
    types: &[(&str, &str)],
    period: Option<(&str, &str)>,
    samples: &[(&[&str], &[i64])],
) -> Profile {
    let mut strings: Vec<String> = vec![String::new()];
    let mut index: HashMap<String, i64> = HashMap::from([(String::new(), 0)]);
    let mut intern = |s: &str, strings: &mut Vec<String>| -> i64 {
        *index.entry(s.to_string()).or_insert_with(|| {
            strings.push(s.to_string());
            (strings.len() - 1) as i64
        })
    };
    let mut p = Profile::default();
    for (t, u) in types {
        let vt = ValueType {
            r#type: intern(t, &mut strings),
            unit: intern(u, &mut strings),
        };
        p.sample_type.push(vt);
    }
    if let Some((t, u)) = period {
        p.period_type = Some(ValueType {
            r#type: intern(t, &mut strings),
            unit: intern(u, &mut strings),
        });
    }
    let mut frames: Vec<String> = Vec::new();
    let mut frame_ids: HashMap<String, u64> = HashMap::new();
    for (stack, _) in samples {
        for f in stack.iter() {
            if !frame_ids.contains_key(*f) {
                frames.push(f.to_string());
                frame_ids.insert(f.to_string(), frames.len() as u64);
            }
        }
    }
    for (i, f) in frames.iter().enumerate() {
        let id = i as u64 + 1;
        match f.strip_prefix("0x") {
            Some(hex) => p.location.push(Location {
                id,
                address: u64::from_str_radix(hex, 16).expect("a hex address"),
                ..Location::default()
            }),
            None => {
                let name = intern(f, &mut strings);
                p.function.push(Function {
                    id,
                    name,
                    ..Function::default()
                });
                p.location.push(Location {
                    id,
                    line: vec![Line {
                        function_id: id,
                        ..Line::default()
                    }],
                    ..Location::default()
                });
            }
        }
    }
    for (stack, values) in samples {
        let ids = stack
            .iter()
            .rev()
            .map(|f| frame_ids[*f])
            .collect();
        p.sample.push(Sample {
            location_id: ids,
            value: values.to_vec(),
            ..Sample::default()
        });
    }
    p.string_table = strings;
    p
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(bytes).expect("gzip");
    e.finish().expect("gzip")
}

fn gz(p: &Profile) -> Vec<u8> {
    gzip(&p.encode_to_vec())
}

const CPU: (&str, &str) = ("cpu", "nanoseconds");
const SAMPLES: (&str, &str) = ("samples", "count");

/// F-P, `p.pb.gz`.
pub fn f_p() -> Vec<u8> {
    gz(&pprof(
        &[CPU, SAMPLES],
        Some(CPU),
        &[
            (&["main", "a"], &[10, 1]),
            (&["main", "b"], &[0, 4]),
            (&["main", "0x1f2e"], &[2, 0]),
        ],
    ))
}

/// F-D, `dup`: two types both named `cpu`.
pub fn f_d() -> Vec<u8> {
    gz(&pprof(
        &[CPU, CPU],
        Some(CPU),
        &[(&["main", "a"], &[10, 1]), (&["main", "b"], &[1, 7])],
    ))
}

/// F-Z, `z`.
pub fn f_z() -> Vec<u8> {
    gz(&pprof(
        &[CPU, SAMPLES],
        Some(CPU),
        &[(&["main", "a"], &[10, 0])],
    ))
}

/// F-BA, `ba`: `samples` before `cpu`.
pub fn f_ba() -> Vec<u8> {
    gz(&pprof(
        &[SAMPLES, CPU],
        Some(CPU),
        &[(&["main", "a"], &[3, 9])],
    ))
}

/// F-E, `empty`: F-P's types, no samples.
pub fn f_e() -> Vec<u8> {
    gz(&pprof(&[CPU, SAMPLES], Some(CPU), &[]))
}

/// F-IDS, `ids`: F-P's strings and functions (`main` 1, `a` 2, `b` 3),
/// the locations listed id 2, 1, 3; samples `[2, 1]` 5 and `[3, 1]` 1.
pub fn f_ids() -> Vec<u8> {
    let mut p = pprof(
        &[CPU],
        Some(CPU),
        &[(&["main", "a"], &[5]), (&["main", "b"], &[1])],
    );
    p.location.swap(0, 1);
    gz(&p)
}

/// F-P's locations with the functions listed id 2, 1, 3, and one sample
/// `main;a` (u4).
pub fn f_fn_order() -> Vec<u8> {
    let mut p = pprof(
        &[CPU],
        Some(CPU),
        &[(&["main", "a"], &[1]), (&["main", "b"], &[0])],
    );
    p.function.swap(0, 1);
    p.sample.truncate(1);
    gz(&p)
}

/// A profile without a period type.
pub fn f_no_period() -> Vec<u8> {
    gz(&pprof(&[CPU], None, &[(&["main"], &[1])]))
}

/// F-BIG: 20 sample types (`t0 count` to `t19 count`), 60,000 distinct
/// one-frame stacks `f0` to `f59999`, every value 1: 1,200,000 sample
/// rows, 20 type rows and 60,000 stack rows.
pub fn f_big() -> Vec<u8> {
    let names: Vec<String> = (0..20).map(|i| format!("t{i}")).collect();
    let types: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "count")).collect();
    let frames: Vec<String> = (0..60_000).map(|i| format!("f{i}")).collect();
    let stacks: Vec<[&str; 1]> = frames.iter().map(|f| [f.as_str()]).collect();
    let values = [1i64; 20];
    let samples: Vec<(&[&str], &[i64])> =
        stacks.iter().map(|s| (&s[..], &values[..])).collect();
    gz(&pprof(&types, Some(("t0", "count")), &samples))
}

/// F-BIG's row count.
pub const F_BIG_ROWS: u64 = 1_260_020;
/// The `maxNodes` F-BIG is read with: every one of its 60,001 nodes kept.
pub const F_BIG_MAX_NODES: i64 = 65_536;

pub const F_C: &[u8] = b"main;a 10\nmain;b 1\n";
pub const F_X: &[u8] = b"main;other;x 5\nmain;y 3\n";
pub const F_X2: &[u8] = b"main;y 1\nmain;z 4\n";
pub const F_J: &[u8] = br#"{"version":1,"flamebearer":{"names":["total","main","a","b"],"levels":[[0,11,0,0],[0,11,1,1],[0,7,7,2,0,3,3,3]],"numTicks":11,"maxSelf":7},"metadata":{"format":"single","spyName":"gospy","sampleRate":250,"units":"bytes","name":"b"}}"#;
pub const F_S: &[u8] = b"java 12345 123.456: 1 cycles:\n\t7f01 foo (/lib/x.so)\n\t7f02 bar (/lib/x.so)\n\njava 12345/12346 124.000: 1 cycles:\n\t7f01 foo (/lib/x.so)\n\n";
pub const F_U_COLLAPSED: &[u8] = "main;中 3\nmain;b 1\n".as_bytes();
pub const F_U_PERF: &[u8] = "java 1 1.0: 1 cycles:\n\t7f01 foo中 (/lib/x.so)\n\n".as_bytes();
pub const F_SE: &[u8] = b"java 12345 123.456: 1 cycles:\n\t7f01 foo (/lib/x.so)\n\njava 12345 124.000: 1 cycles:\n\t7f02 bar (/lib/x.so)";

/// The base64 the app sends.
pub fn b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
