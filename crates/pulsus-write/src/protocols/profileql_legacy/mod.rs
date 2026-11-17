//! `/ingest`'s converted formats: folded stacks (`groups`), individual lines,
//! the trie and tree binaries, and speedscope JSON. Each body becomes a
//! [`StackSet`], and [`put_input`] turns a stack set into the
//! [`ProfileInput`] the pprof model takes, as the reference's ingester
//! adapter turns its tree into a pprof profile
//! (`pkg/ingester/…/ingest_adapter.go:77-176`,
//! `pkg/og/storage/tree/pprof.go:103-180`). Pure functions, no I/O.

pub mod folded;
pub mod speedscope;
pub mod tree;
pub mod trie;

use std::collections::HashMap;

use prost::Message;
use pulsus_profiles_wire::google_v1::{
    Function, Line, Location, Mapping, Profile, Sample, ValueType,
};

use crate::ingest::profiles::ProfileInput;
use crate::protocols::profile_model::{MAX_PROFILE_SIZE_BYTES, SPY_LABEL};
use crate::protocols::profile_push::{Labels, LegacyQuery};

/// The reference adapter's type name, which its error texts carry
/// (`ingest_adapter.go:47`).
pub const ADAPTER: &str = "pyroscopeIngesterAdapter";

/// Why a converted body stored nothing, by the family its answer takes
/// (§3.5 of the design for issue #818).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyError {
    /// The body does not parse.
    Parse(String),
    /// The name's type or a frame name refused while building the profile.
    Metadata(String),
    /// The built profile is over the model's size limit.
    Size(String),
}

/// A body's stacks: each distinct frame name once, each distinct stack once
/// with its value summed. The reference tree's insert and iterate, without
/// the tree (`pkg/og/storage/tree/tree.go:213-251`).
#[derive(Debug, Default)]
pub struct StackSet {
    names: HashMap<Box<[u8]>, u64>,
    stacks: HashMap<Vec<u64>, u64>,
    scratch: Vec<u64>,
}

impl StackSet {
    /// Adds `v` to the stack `text`, its frames split on `;`: the last piece
    /// is dropped when empty, so `a;b;` is `a;b`, `""` has no frame and `";"`
    /// one empty frame. A zero adds nothing, since a zero stack is never a
    /// sample.
    pub fn add_text(&mut self, text: &[u8], v: u64) {
        if v == 0 {
            return;
        }
        self.scratch.clear();
        let mut pieces = text.split(|&b| b == b';').peekable();
        while let Some(frame) = pieces.next() {
            if pieces.peek().is_none() && frame.is_empty() {
                break;
            }
            let id = self.name_id(frame);
            self.scratch.push(id);
        }
        self.commit(v);
    }

    /// Adds `v` to the stack `frames`, each taken whole.
    pub fn add_frames(&mut self, frames: &[&str], v: u64) {
        if v == 0 {
            return;
        }
        self.scratch.clear();
        for frame in frames {
            let id = self.name_id(frame.as_bytes());
            self.scratch.push(id);
        }
        self.commit(v);
    }

    /// The frame's id, 1-based in first-seen order.
    fn name_id(&mut self, name: &[u8]) -> u64 {
        if let Some(&id) = self.names.get(name) {
            return id;
        }
        let id = self.names.len() as u64 + 1;
        self.names.insert(name.into(), id);
        id
    }

    /// Adds `v` to the stack in `scratch`, wrapping as the reference's
    /// unsigned sums do.
    fn commit(&mut self, v: u64) {
        match self.stacks.get_mut(self.scratch.as_slice()) {
            Some(sum) => *sum = sum.wrapping_add(v),
            None => {
                self.stacks.insert(self.scratch.clone(), v);
            }
        }
    }
}

/// A cursor over a binary body, failing as Go's readers fail.
pub(crate) struct Reader<'a> {
    body: &'a [u8],
    pos: usize,
}

/// Go's `io.EOF`: the read found no byte at all.
pub(crate) const EOF: &str = "EOF";

impl<'a> Reader<'a> {
    pub(crate) fn new(body: &'a [u8]) -> Self {
        Reader { body, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.body.len() - self.pos
    }

    /// `binary.ReadUvarint`: `EOF` before the first byte, `unexpected EOF`
    /// after it, and an overflow past 64 bits refused with Go's text.
    pub(crate) fn uvarint(&mut self) -> Result<u64, &'static str> {
        let mut x: u64 = 0;
        for i in 0..10 {
            let Some(&b) = self.body.get(self.pos) else {
                return Err(if i == 0 { EOF } else { "unexpected EOF" });
            };
            self.pos += 1;
            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err("binary: varint overflows a 64-bit integer");
                }
                return Ok(x | u64::from(b) << (7 * i));
            }
            x |= u64::from(b & 0x7f) << (7 * i);
        }
        Err("binary: varint overflows a 64-bit integer")
    }

    /// Up to `n` bytes, fewer at the end of the body.
    pub(crate) fn take_up_to(&mut self, n: usize) -> &'a [u8] {
        let n = n.min(self.remaining());
        let out = &self.body[self.pos..self.pos + n];
        self.pos += n;
        out
    }

    /// `io.ReadAtLeast` of exactly `n` bytes: `EOF` when none is left,
    /// `unexpected EOF` when some but too few are.
    pub(crate) fn take_exact(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        if n > self.remaining() {
            return Err(if self.remaining() == 0 {
                EOF
            } else {
                "unexpected EOF"
            });
        }
        Ok(self.take_up_to(n))
    }
}

fn parse_error(e: &str) -> LegacyError {
    LegacyError::Parse(e.to_string())
}

/// The metric, sample type and unit of a type key
/// (`ingest_adapter.go:208-306`).
fn type_of(key: &str) -> Option<(&'static str, &str, &'static str)> {
    const COUNT: &str = "count";
    const BYTES: &str = "bytes";
    Some(match key {
        "cpu" | "itimer" => ("process_cpu", "samples", COUNT),
        "wall" | "seconds" | "nanoseconds" | "microseconds" | "milliseconds" => {
            ("wall", "samples", COUNT)
        }
        "inuse_objects"
        | "alloc_objects"
        | "alloc_in_new_tlab_objects"
        | "alloc_outside_tlab_objects"
        | "live" => ("memory", key, COUNT),
        "inuse_space" | "alloc_space" | "alloc_in_new_tlab_bytes" | "alloc_outside_tlab_bytes" => {
            ("memory", key, BYTES)
        }
        "bytes" => ("memory", "samples", BYTES),
        "goroutines" => ("goroutine", "goroutines", COUNT),
        "mutex_count" => ("mutex", "contentions", COUNT),
        "mutex_duration" => ("mutex", "delay", "nanoseconds"),
        "block_count" | "lock_count" => ("block", "contentions", COUNT),
        "block_duration" | "lock_duration" => ("block", "delay", "nanoseconds"),
        "exceptions" => ("exceptions", "samples", COUNT),
        _ => return None,
    })
}

/// A string table that gives the few type and unit strings one index each,
/// as the reference's interning does (`pprof.go:141-149`); frame names are
/// distinct already.
struct Strings {
    table: Vec<String>,
}

impl Strings {
    fn index(&mut self, s: &str) -> i64 {
        if let Some(i) = self.table.iter().position(|t| t == s) {
            return i as i64;
        }
        self.table.push(s.to_string());
        (self.table.len() - 1) as i64
    }
}

/// One stack set as the model's input (§3.3): its type from `name`'s
/// service, its values scaled by the sample rate, and the reference's labels
/// (`ingest_adapter.go:77-176`).
pub fn put_input(
    set: StackSet,
    query: &LegacyQuery,
    name: &Labels,
) -> Result<ProfileInput, LegacyError> {
    let service = name
        .iter()
        .find(|(k, _)| k == "__name__")
        .map_or("", |(_, v)| v.as_str());
    let (key, app) = match service.rsplit_once('.') {
        Some((app, key)) => (key, app),
        None => ("cpu", service),
    };
    let (metric, mut st_type, mut st_unit) = type_of(key).ok_or_else(|| {
        LegacyError::Metadata(format!(
            "{ADAPTER} failed to convert metadata: unknown profile type: {key}"
        ))
    })?;
    let mut scale: u64 = 1;
    let mut period: i64 = 0;
    if query.sample_rate != 0 && (metric == "wall" || metric == "process_cpu") {
        period = 1_000_000_000 / i64::from(query.sample_rate);
        st_type = if metric == "wall" { "wall" } else { "cpu" };
        st_unit = "nanoseconds";
        scale = period as u64;
    }

    let mut strings = Strings {
        table: vec![String::new()],
    };
    let sample_type = vec![ValueType {
        r#type: strings.index(st_type),
        unit: strings.index(st_unit),
    }];
    let period_type = (period != 0).then(|| ValueType {
        r#type: strings.index("cpu"),
        unit: strings.index("nanoseconds"),
    });
    let meta = strings.table.len();

    let StackSet { names, stacks, .. } = set;
    let mut by_id: Vec<Option<Box<[u8]>>> = (0..names.len()).map(|_| None).collect();
    for (name, id) in names {
        by_id[(id - 1) as usize] = Some(name);
    }
    let mut function = Vec::with_capacity(by_id.len());
    let mut location = Vec::with_capacity(by_id.len());
    for (i, name) in by_id.into_iter().enumerate() {
        let id = i as u64 + 1;
        let name = String::from_utf8(name.unwrap_or_default().into_vec()).map_err(|_| {
            LegacyError::Metadata(format!(
                "{ADAPTER} failed to marshal pprof: string field contains invalid UTF-8"
            ))
        })?;
        let index = match strings.table[..meta].iter().position(|t| *t == name) {
            Some(i) => i as i64,
            None => {
                strings.table.push(name);
                (strings.table.len() - 1) as i64
            }
        };
        function.push(Function {
            id,
            name: index,
            system_name: index,
            ..Function::default()
        });
        location.push(Location {
            id,
            mapping_id: 1,
            line: vec![Line {
                function_id: id,
                line: 0,
            }],
            ..Location::default()
        });
    }
    let mut sample = Vec::with_capacity(stacks.len());
    for (mut stack, v) in stacks {
        let value = v.wrapping_mul(scale) as i64;
        if value == 0 {
            continue;
        }
        stack.reverse();
        sample.push(Sample {
            location_id: stack,
            value: vec![value],
            label: Vec::new(),
        });
    }
    let profile = Profile {
        sample_type,
        sample,
        mapping: vec![Mapping {
            id: 1,
            ..Mapping::default()
        }],
        location,
        function,
        string_table: strings.table,
        time_nanos: query.from_ns.unwrap_or(0),
        period_type,
        period,
        ..Profile::default()
    };
    let size = profile.encoded_len() as u64;
    if size > MAX_PROFILE_SIZE_BYTES {
        return Err(LegacyError::Size(
            "uncompressed profile payload size exceeds limit of 4.2 MB".to_string(),
        ));
    }

    let mut labels: Labels = vec![
        ("__name__".to_string(), metric.to_string()),
        ("__delta__".to_string(), "false".to_string()),
    ];
    if !query.spy_name.is_empty() {
        labels.push((SPY_LABEL.to_string(), query.spy_name.clone()));
    }
    labels.extend(
        name.iter()
            .filter(|(k, _)| !k.starts_with("__") || k == "__session_id__")
            .cloned(),
    );
    if !name.iter().any(|(k, _)| k == "service_name") {
        labels.push(("service_name".to_string(), app.to_string()));
    } else if !name.iter().any(|(k, _)| k == "app_name") {
        labels.push(("app_name".to_string(), app.to_string()));
    }
    Ok(ProfileInput {
        labels,
        profile,
        decompressed_bytes: size,
        id: None,
        annotations: Vec::new(),
        language: None,
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared by the format modules' tests.

    use std::collections::BTreeMap;

    use super::*;
    use crate::protocols::profile_push::parse_name;

    /// Root-first frame names to stored value.
    pub(crate) type Stacks = BTreeMap<Vec<String>, i64>;

    /// The reference's trie bytes (`transporttrie/trie_test.go:49`): `abc` 1,
    /// `abd` 2.
    pub(crate) const TRIE: &[u8] = b"\x00\x00\x01\x02ab\x00\x02\x01c\x01\x00\x01d\x02\x00";

    /// The reference's tree bytes (`tree/serialize_nodict_test.go:10`):
    /// `a;b` 1, `a;c` 2.
    pub(crate) const TREE: &[u8] = b"\x00\x00\x01\x01a\x00\x02\x01b\x01\x00\x01c\x02\x00";

    /// The reference's `speedscope/testdata/simple.speedscope.json`.
    pub(crate) const SIMPLE: &str = r#"{
  "$schema": "https://www.speedscope.app/file-format-schema.json",
  "profiles": [
    {
      "endValue": 14,
      "events": [
        { "at": 0, "frame": 0, "type": "O" },
        { "at": 0, "frame": 1, "type": "O" },
        { "at": 0, "frame": 2, "type": "O" },
        { "at": 2, "frame": 2, "type": "C" },
        { "at": 2, "frame": 3, "type": "O" },
        { "at": 6, "frame": 3, "type": "C" },
        { "at": 6, "frame": 2, "type": "O" },
        { "at": 9, "frame": 2, "type": "C" },
        { "at": 14, "frame": 1, "type": "C" },
        { "at": 14, "frame": 0, "type": "C" }
      ],
      "name": "simple.txt",
      "startValue": 0,
      "type": "evented",
      "unit": "none"
    }
  ],
  "shared": {
    "frames": [{ "name": "a" }, { "name": "b" }, { "name": "c" }, { "name": "d" }]
  },
  "version": "0.0.1"
}"#;

    /// One sampled profile of the reference's
    /// `speedscope/testdata/two-sampled.speedscope.json`, named `name`.
    pub(crate) fn sampled_profile(name: &str, end: u32) -> String {
        format!(
            r#"{{
      "type": "sampled",
      "name": "{name}",
      "unit": "seconds",
      "startValue": 0,
      "endValue": {end},
      "samples": [[0, 1, 2], [0, 1, 2], [0, 1, 3], [0, 1, 2], [0, 1]],
      "weights": [1, 1, 4, 3, 5]
    }}"#
        )
    }

    /// A speedscope file over the frames `a b c d` with `profiles`.
    pub(crate) fn speedscope_file(profiles: &[String]) -> String {
        format!(
            r#"{{
  "exporter": "speedscope@0.6.0",
  "$schema": "https://www.speedscope.app/file-format-schema.json",
  "name": "Two Samples",
  "activeProfileIndex": 1,
  "profiles": [{}],
  "shared": {{
    "frames": [{{ "name": "a" }}, {{ "name": "b" }}, {{ "name": "c" }}, {{ "name": "d" }}]
  }}
}}"#,
            profiles.join(",")
        )
    }

    /// The reference's `speedscope/testdata/two-sampled.speedscope.json`.
    pub(crate) fn two_sampled() -> String {
        speedscope_file(&[sampled_profile("one", 14), sampled_profile("two", 14)])
    }

    pub(crate) fn query(name: &str, sample_rate: u32) -> LegacyQuery {
        LegacyQuery {
            name: parse_name(name).expect("a valid name"),
            from_ns: None,
            spy_name: "unknown".to_string(),
            format: None,
            sample_rate,
        }
    }

    pub(crate) fn map(entries: &[(&[&str], i64)]) -> Stacks {
        entries
            .iter()
            .map(|(k, v)| (k.iter().map(|s| s.to_string()).collect(), *v))
            .collect()
    }

    /// The input's samples, read through its locations and functions.
    pub(crate) fn stacks(input: &ProfileInput) -> Stacks {
        let p = &input.profile;
        let name_of = |loc: u64| {
            let l = p.location.iter().find(|l| l.id == loc).expect("location");
            let f = p
                .function
                .iter()
                .find(|f| f.id == l.line[0].function_id)
                .expect("function");
            p.string_table[f.name as usize].clone()
        };
        let mut out = Stacks::new();
        for s in &p.sample {
            let names: Vec<String> = s.location_id.iter().rev().map(|&l| name_of(l)).collect();
            *out.entry(names).or_insert(0) += s.value[0];
        }
        out
    }

    /// `set` as `name`'s input at `sample_rate`, read as stacks.
    pub(crate) fn stacks_as(
        set: Result<StackSet, LegacyError>,
        name: &str,
        sample_rate: u32,
    ) -> Result<Stacks, LegacyError> {
        let q = query(name, sample_rate);
        put_input(set?, &q, &q.name).map(|i| stacks(&i))
    }

    /// [`stacks_as`] under `app` at the default rate.
    pub(crate) fn stacks_of(set: Result<StackSet, LegacyError>) -> Result<Stacks, LegacyError> {
        stacks_as(set, "app", 100)
    }

    /// `type/unit` of the input's one sample type.
    pub(crate) fn sample_type(input: &ProfileInput) -> String {
        let p = &input.profile;
        let st = &p.sample_type[0];
        format!(
            "{}/{}",
            p.string_table[st.r#type as usize], p.string_table[st.unit as usize]
        )
    }

    /// The period and `type/unit` of the period type, when there is one.
    pub(crate) fn period(input: &ProfileInput) -> (i64, Option<String>) {
        let p = &input.profile;
        let pt = p.period_type.map(|pt| {
            format!(
                "{}/{}",
                p.string_table[pt.r#type as usize], p.string_table[pt.unit as usize]
            )
        });
        (p.period, pt)
    }

    pub(crate) fn label<'a>(input: &'a ProfileInput, key: &str) -> Option<&'a str> {
        input
            .labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::folded::parse_groups;
    use super::test_support::*;
    use super::*;

    fn input(body: &[u8], name: &str, sample_rate: u32) -> Result<ProfileInput, LegacyError> {
        let q = query(name, sample_rate);
        put_input(parse_groups(body)?, &q, &q.name)
    }

    #[test]
    fn f7_the_type_comes_from_the_names_last_part() {
        let rows: [(&str, &str, &str); 22] = [
            ("cpu", "process_cpu", "samples/count"),
            ("itimer", "process_cpu", "samples/count"),
            ("wall", "wall", "samples/count"),
            ("seconds", "wall", "samples/count"),
            ("nanoseconds", "wall", "samples/count"),
            ("microseconds", "wall", "samples/count"),
            ("milliseconds", "wall", "samples/count"),
            ("inuse_objects", "memory", "inuse_objects/count"),
            ("alloc_objects", "memory", "alloc_objects/count"),
            ("inuse_space", "memory", "inuse_space/bytes"),
            ("alloc_space", "memory", "alloc_space/bytes"),
            (
                "alloc_in_new_tlab_objects",
                "memory",
                "alloc_in_new_tlab_objects/count",
            ),
            (
                "alloc_outside_tlab_objects",
                "memory",
                "alloc_outside_tlab_objects/count",
            ),
            ("live", "memory", "live/count"),
            (
                "alloc_in_new_tlab_bytes",
                "memory",
                "alloc_in_new_tlab_bytes/bytes",
            ),
            (
                "alloc_outside_tlab_bytes",
                "memory",
                "alloc_outside_tlab_bytes/bytes",
            ),
            ("bytes", "memory", "samples/bytes"),
            ("goroutines", "goroutine", "goroutines/count"),
            ("mutex_count", "mutex", "contentions/count"),
            ("mutex_duration", "mutex", "delay/nanoseconds"),
            ("block_count", "block", "contentions/count"),
            ("block_duration", "block", "delay/nanoseconds"),
        ];
        let more: [(&str, &str, &str); 3] = [
            ("lock_count", "block", "contentions/count"),
            ("lock_duration", "block", "delay/nanoseconds"),
            ("exceptions", "exceptions", "samples/count"),
        ];
        for (key, metric, st) in rows.iter().chain(more.iter()) {
            let got = input(b"a 3\n", &format!("app.{key}"), 0)
                .map(|i| (label(&i, "__name__").map(str::to_string), sample_type(&i)));
            assert_eq!(got, Ok((Some(metric.to_string()), st.to_string())), "{key}");
        }

        let got = input(b"a 3\n", "app", 0).map(|i| (sample_type(&i), period(&i), stacks(&i)));
        assert_eq!(
            got,
            Ok(("samples/count".to_string(), (0, None), map(&[(&["a"], 3)])))
        );
        let got = input(b"a 3\n", "app", 1000).map(|i| (period(&i), stacks(&i)));
        assert_eq!(
            got,
            Ok((
                (1_000_000, Some("cpu/nanoseconds".to_string())),
                map(&[(&["a"], 3_000_000)])
            ))
        );
        let got = input(b"a 3\n", "app.inuse_space", 100).map(|i| {
            (
                label(&i, "__name__").map(str::to_string),
                sample_type(&i),
                stacks(&i),
            )
        });
        assert_eq!(
            got,
            Ok((
                Some("memory".to_string()),
                "inuse_space/bytes".to_string(),
                map(&[(&["a"], 3)])
            ))
        );
    }

    #[test]
    fn f8_the_service_and_app_labels_follow_the_reference() {
        let labels = |name: &str| {
            input(b"a 1\n", name, 100).map(|i| {
                let mut ls = i.labels.clone();
                ls.sort();
                ls
            })
        };
        let pairs = |ls: &[(&str, &str)]| {
            let mut v: Vec<(String, String)> = ls
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            v.sort();
            v
        };
        let base = [
            ("__name__", "process_cpu"),
            ("__delta__", "false"),
            (crate::protocols::profile_model::SPY_LABEL, "unknown"),
        ];
        let with = |more: &[(&'static str, &'static str)]| {
            let mut all = base.to_vec();
            all.extend_from_slice(more);
            pairs(&all)
        };
        assert_eq!(labels("app"), Ok(with(&[("service_name", "app")])));
        assert_eq!(
            labels("app{service_name=svc}"),
            Ok(with(&[("service_name", "svc"), ("app_name", "app")]))
        );
        assert_eq!(
            labels("app{service_name=svc,app_name=x}"),
            Ok(with(&[("service_name", "svc"), ("app_name", "x")]))
        );
        assert_eq!(
            labels("app.cpu{__session_id__=s,__x__=y}"),
            Ok(with(&[("__session_id__", "s"), ("service_name", "app")]))
        );
    }

    #[test]
    fn f9_an_unknown_type_is_a_metadata_error() {
        assert_eq!(
            input(b"a 1\n", "my.app", 100).map(|i| i.labels),
            Err(LegacyError::Metadata(
                "pyroscopeIngesterAdapter failed to convert metadata: unknown profile type: app"
                    .to_string()
            ))
        );
    }

    #[test]
    fn f10_a_profile_id_label_is_stored_as_a_label() {
        let got = input(b"a 1\n", "app{profile_id=p1}", 100)
            .map(|i| (label(&i, "profile_id").map(str::to_string), stacks(&i)));
        assert_eq!(
            got,
            Ok((Some("p1".to_string()), map(&[(&["a"], 10_000_000)])))
        );
    }
}
