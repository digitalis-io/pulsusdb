// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Issue #829 part B: the merged profile as a DOT graph. Stubs: the tests
//! come first.

#[allow(dead_code)]
mod measurement;

use pulsus_profiles_wire::google_v1::{Mapping, Profile};

pub const DOT_MAX_NODES_DEFAULT: i64 = 100;
pub const SOURCE_MAX_NODES: i64 = 512;

/// Stub.
pub fn limits(_max_nodes: Option<i64>) -> (i64, i64) {
    (0, 0)
}

/// Stub.
pub fn write(_p: &Profile, _max_nodes: i64) -> Option<String> {
    let _: Option<Mapping> = None;
    Some(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_profiles_wire::google_v1::{Function, Line, Location, Sample, ValueType};

    /// 2026-11-20 19:53:50 UTC, the reference captures' profile time.
    const TIME_NANOS: i64 = 1_795_204_430_000_000_000;

    /// A profile's tables under construction.
    #[derive(Default)]
    struct Tables {
        strings: Vec<String>,
        functions: Vec<Function>,
    }

    impl Tables {
        fn string(&mut self, s: &str) -> i64 {
            if let Some(i) = self.strings.iter().position(|x| x == s) {
                return i as i64;
            }
            self.strings.push(s.to_string());
            self.strings.len() as i64 - 1
        }

        /// The function `(name, file, start line)`, its system name its
        /// name.
        fn function(&mut self, name: &str, file: &str, start: i64) -> u64 {
            let (n, f) = (self.string(name), self.string(file));
            if let Some(x) = self
                .functions
                .iter()
                .find(|x| x.name == n && x.filename == f && x.start_line == start)
            {
                return x.id;
            }
            let id = self.functions.len() as u64 + 1;
            self.functions.push(Function {
                id,
                name: n,
                system_name: n,
                filename: f,
                start_line: start,
            });
            id
        }
    }

    fn profile(
        t: Tables,
        ty: (i64, i64),
        mapping: Vec<Mapping>,
        location: Vec<Location>,
        sample: Vec<Sample>,
    ) -> Profile {
        Profile {
            sample_type: vec![ValueType {
                r#type: ty.0,
                unit: ty.1,
            }],
            sample,
            mapping,
            location,
            function: t.functions,
            string_table: t.strings,
            time_nanos: TIME_NANOS,
            ..Profile::default()
        }
    }

    /// One location per distinct frame name, no mapping, address 0, one
    /// line `{function, 0}`; values `count × unit`.
    fn folded(stacks: &[(&str, i64)], unit: i64, ty: (&str, &str)) -> Profile {
        let mut t = Tables::default();
        t.string("");
        let ty = (t.string(ty.0), t.string(ty.1));
        let mut names: Vec<String> = Vec::new();
        let mut location = Vec::new();
        let mut sample = Vec::new();
        for (stack, count) in stacks {
            let mut ids = Vec::new();
            for frame in stack.split(';') {
                let id = match names.iter().position(|n| n == frame) {
                    Some(i) => i as u64 + 1,
                    None => {
                        names.push(frame.to_string());
                        let id = names.len() as u64;
                        let f = t.function(frame, "", 0);
                        location.push(Location {
                            id,
                            line: vec![Line {
                                function_id: f,
                                line: 0,
                            }],
                            ..Location::default()
                        });
                        id
                    }
                };
                ids.push(id);
            }
            ids.reverse();
            sample.push(Sample {
                location_id: ids,
                value: vec![count * unit],
                label: Vec::new(),
            });
        }
        profile(t, ty, Vec::new(), location, sample)
    }

    const TRIM: [(&str, i64); 6] = [
        ("main;a;b", 17),
        ("main;a;c", 5),
        ("main;d", 3),
        ("main;a", 2),
        ("main;e;f", 1),
        ("main;other;x", 4),
    ];
    const CPU: (&str, &str) = ("cpu", "nanoseconds");
    const TEN_MS: i64 = 10_000_000;

    fn assert_text(got: Option<String>, want: &str, ctx: &str) {
        let got = got.unwrap_or_else(|| panic!("{ctx}: no text"));
        assert!(
            got == want,
            "{ctx}:\n--- got ---\n{got}--- want ---\n{want}"
        );
    }

    #[test]
    fn d1_folded() {
        let p = folded(&TRIM, TEN_MS, CPU);
        assert_text(write(&p, 100), include_str!("testdata/a1.dot"), "d1");
    }

    #[test]
    fn d2_top_four() {
        let p = folded(&TRIM, TEN_MS, CPU);
        assert_text(write(&p, 4), include_str!("testdata/a2.dot"), "d2");
    }

    #[test]
    fn d3_names() {
        let p = folded(
            &[
                (
                    "main;github.com/x/y/v2/pkg.(*T).Run;com.example.Foo.bar;\
                     ns::Widget::draw<int>;(anonymous namespace)::helper",
                    99_999,
                ),
                ("main;tiny", 1),
                ("main;mid", 300),
            ],
            TEN_MS,
            CPU,
        );
        assert_text(write(&p, 100), include_str!("testdata/a3.dot"), "d3");
    }

    #[test]
    fn d4_bytes() {
        let p = folded(
            &[
                ("main;alloc", 1536),
                ("main;small", 3),
                ("main;big", 3_221_225_472),
            ],
            1,
            ("alloc_space", "bytes"),
        );
        assert_text(write(&p, 100), include_str!("testdata/a4.dot"), "d4");
    }

    /// The legend takes `app`, the second mapping, over `libx.so`, the
    /// first; nodes carry `file:line`; the cut leaves a residual edge.
    #[test]
    fn d5_mapping_lines_residual() {
        let mut t = Tables::default();
        t.string("");
        let ty = (t.string("cpu"), t.string("nanoseconds"));
        let mapping = vec![
            Mapping {
                id: 1,
                filename: t.string("libx.so"),
                ..Mapping::default()
            },
            Mapping {
                id: 2,
                filename: t.string("app"),
                build_id: t.string("bid"),
                ..Mapping::default()
            },
        ];
        let lines: [(&str, i64, i64); 8] = [
            ("main", 10, 1),
            ("a", 20, 2),
            ("a", 21, 2),
            ("b", 30, 3),
            ("c", 40, 4),
            ("d", 50, 5),
            ("e", 60, 6),
            ("zz", 70, 7),
        ];
        let mut location: Vec<Location> = lines
            .iter()
            .enumerate()
            .map(|(i, (name, line, start))| {
                let f = t.function(name, "main.go", *start);
                Location {
                    id: i as u64 + 1,
                    mapping_id: 2,
                    line: vec![Line {
                        function_id: f,
                        line: *line,
                    }],
                    ..Location::default()
                }
            })
            .collect();
        let other = t.function("other", "", 0);
        location.push(Location {
            id: 9,
            line: vec![Line {
                function_id: other,
                line: 0,
            }],
            ..Location::default()
        });
        let samples: [(&[u64], i64); 7] = [
            (&[2, 1], 4),
            (&[3, 1], 6),
            (&[4, 2, 1], 20),
            (&[5, 3, 1], 2),
            (&[7, 6, 1], 8),
            (&[8], 2),
            (&[9], 12),
        ];
        let sample = samples
            .iter()
            .map(|(ids, count)| Sample {
                location_id: ids.to_vec(),
                value: vec![count * TEN_MS],
                label: Vec::new(),
            })
            .collect();
        let p = profile(t, ty, mapping, location, sample);
        assert_text(write(&p, 3), include_str!("testdata/a5.dot"), "d5");
    }

    #[test]
    fn d6_limits() {
        for n in [None, Some(0), Some(-5)] {
            assert_eq!(limits(n), (100, 512), "{n:?}");
        }
        assert_eq!(limits(Some(4)), (4, 512));
        assert_eq!(limits(Some(1000)), (1000, 1000));
    }

    #[test]
    fn d7_no_samples() {
        assert_eq!(write(&Profile::default(), 100), None);
    }
}
