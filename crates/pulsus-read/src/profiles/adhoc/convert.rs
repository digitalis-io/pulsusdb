// Ported from the reference at tag v2.4.3: pkg/og/structs/flamebearer/convert/convert.go and pkg/og/convert/perf/script.go, under the GNU Affero General Public License v3.0, the licence of this repository.
// Changed for PulsusDB, issue #831: ported from Go to Rust; a pprof's locations and functions found by id; a perf script's last event kept at the end of the text.

//! Issue #831: an uploaded file's format (plan §4.1) and its conversion
//! into types and stacks (§4.2). Pure: no I/O.

/// An upload's format, as the reference names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Pprof,
    Collapsed,
    PerfScript,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Pprof => "pprof",
            Format::Collapsed => "collapsed",
            Format::PerfScript => "perf_script",
        }
    }
}

/// One type of a converted upload: its metadata and its stacks, frames
/// root first, each with its value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConvertedType {
    pub display_name: String,
    pub spy_name: String,
    pub sample_rate: u32,
    pub units: String,
    pub stacks: Vec<(Vec<String>, i64)>,
}

/// A converted upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
    pub format: Format,
    pub types: Vec<ConvertedType>,
}

/// Why a conversion refused a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    /// A pprof over 4 MiB decompressed.
    TooLarge,
    /// Any other refusal, with the converter's text.
    Invalid(String),
}

/// The largest pprof, decompressed, an upload may carry.
pub const MAX_PPROF_BYTES: u64 = 4_194_304;

/// The format of `bytes` uploaded as `name` (§4.1, `convert.go:124-164`).
pub fn detect(name: &str, bytes: &[u8]) -> Result<Format, String> {
    let ext = extension(name);
    match ext {
        "json" => return Ok(Format::Json),
        "pprof" => return Ok(Format::Pprof),
        "collapsed" => return Ok(Format::Collapsed),
        "perf_script" => return Ok(Format::PerfScript),
        "txt" => {
            return Ok(if is_perf_script(bytes) {
                Format::PerfScript
            } else {
                Format::Collapsed
            });
        }
        _ => {}
    }
    if bytes.len() < 2 {
        return Err("profile is too short".to_string());
    }
    if bytes[0] == b'{' {
        return Ok(Format::Json);
    }
    if bytes[0] == 0x1f && bytes[1] == 0x8b {
        return Ok(Format::Pprof);
    }
    if bytes.iter().take(100).any(|&b| !printable_or_space(b)) {
        return Ok(Format::Pprof);
    }
    Ok(if is_perf_script(bytes) {
        Format::PerfScript
    } else {
        Format::Collapsed
    })
}

/// Go's `path.Ext` without its dot: the text after the last `.` of the
/// last path element, or nothing.
fn extension(name: &str) -> &str {
    let base = name.rsplit('/').next().unwrap_or(name);
    base.rfind('.').map(|i| &base[i + 1..]).unwrap_or("")
}

/// Go's `unicode.IsPrint(rune(b)) || unicode.IsSpace(rune(b))` for one byte
/// read as a Latin-1 code point.
fn printable_or_space(b: u8) -> bool {
    matches!(b, 0x09..=0x0d | 0x20..=0x7e | 0x85 | 0xa0..=0xac | 0xae..=0xff)
}

/// The perf script event line (`script.go:11`), RE2's ASCII classes.
fn event_re() -> &'static regex::bytes::Regex {
    static RE: std::sync::OnceLock<regex::bytes::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        regex::bytes::Regex::new(
            r"(?-u)^([^\t\n\f\r ].+?)[\t\n\f\r ]+([0-9]+)/*([0-9]+)*[\t\n\f\r ]+[^\t\n\f\r ].+",
        )
        .expect("the event pattern compiles")
    })
}

/// The perf script stack frame line (`script.go:13`).
fn frame_re() -> &'static regex::bytes::Regex {
    static RE: std::sync::OnceLock<regex::bytes::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        regex::bytes::Regex::new(
            r"(?-u)^[\t\n\f\r ]*([0-9A-Za-z_]+)[\t\n\f\r ]*(.+) \(([^\t\n\f\r ]*)\)",
        )
        .expect("the frame pattern compiles")
    })
}

/// Whether the text holds a `\n` and the line before it is a perf script's
/// event line (`script.go:87-95`).
pub fn is_perf_script(bytes: &[u8]) -> bool {
    match bytes.iter().position(|&b| b == b'\n') {
        Some(i) => event_start(&bytes[..i]).is_ok(),
        None => false,
    }
}

/// An event line's command (`parseEventStart`).
fn event_start(line: &[u8]) -> Result<&[u8], String> {
    let caps = event_re()
        .captures(line)
        .ok_or_else(|| "reEventStart mismatch".to_string())?;
    for group in [2, 3] {
        if let Some(m) = caps.get(group) {
            go_atoi(m.as_bytes())?;
        }
    }
    Ok(caps.get(1).map(|m| m.as_bytes()).unwrap_or_default())
}

/// Go's `strconv.Atoi` refusals for a run of ASCII digits.
fn go_atoi(digits: &[u8]) -> Result<i64, String> {
    let text = String::from_utf8_lossy(digits);
    text.parse::<i64>()
        .map_err(|_| format!("strconv.Atoi: parsing {text:?}: value out of range"))
}

/// `bytes`, uploaded as `name`, converted as `format` (§4.2).
pub fn convert(format: Format, name: &str, bytes: &[u8]) -> Result<Converted, ConvertError> {
    let invalid = ConvertError::Invalid;
    let types = match format {
        Format::Pprof => pprof(bytes)?,
        Format::Collapsed => vec![text_type(name, collapsed(bytes).map_err(invalid)?)],
        Format::PerfScript => vec![text_type(name, perf_script(bytes).map_err(invalid)?)],
        Format::Json => vec![json(bytes).map_err(invalid)?],
    };
    Ok(Converted { format, types })
}

/// A collapsed or perf script upload's one type (`convert.go:316-325`).
fn text_type(name: &str, stacks: Vec<(Vec<String>, i64)>) -> ConvertedType {
    ConvertedType {
        display_name: name.to_string(),
        spy_name: "unknown".to_string(),
        sample_rate: 100,
        units: String::new(),
        stacks,
    }
}

/// Sums equal stacks, in the order each is first met; a stack of no
/// frames is not kept.
#[derive(Default)]
struct Stacks {
    order: Vec<(Vec<String>, i64)>,
    index: std::collections::HashMap<Vec<String>, usize>,
}

impl Stacks {
    fn add(&mut self, frames: Vec<String>, v: i64) {
        if frames.is_empty() {
            return;
        }
        match self.index.get(&frames) {
            Some(&i) => self.order[i].1 += v,
            None => {
                self.index.insert(frames.clone(), self.order.len());
                self.order.push((frames, v));
            }
        }
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// `CollapsedToProfile` (`convert.go:301-327`): a line's stack before its
/// last space, split on `;` with a trailing empty piece dropped, as
/// `tree.Insert` splits it; its value a `uint64`.
fn collapsed(bytes: &[u8]) -> Result<Vec<(Vec<String>, i64)>, String> {
    let mut stacks = Stacks::default();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let i = line
            .iter()
            .rposition(|&b| b == b' ')
            .ok_or_else(|| "unable to find stacktrace and value separator".to_string())?;
        let value =
            parse_uint(&line[i + 1..]).map_err(|e| format!("unable to parse sample value: {e}"))?;
        let mut frames: Vec<&[u8]> = line[..i].split(|&b| b == b';').collect();
        if frames.last().is_some_and(|f| f.is_empty()) {
            frames.pop();
        }
        stacks.add(frames.into_iter().map(text).collect(), value as i64);
    }
    Ok(stacks.order)
}

/// Go's `strconv.ParseUint(s, 10, 64)` and its texts.
fn parse_uint(b: &[u8]) -> Result<u64, String> {
    let s = text(b);
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return Err(format!(
            "strconv.ParseUint: parsing {}: invalid syntax",
            go_quote(&s)
        ));
    }
    s.parse::<u64>().map_err(|_| {
        format!(
            "strconv.ParseUint: parsing {}: value out of range",
            go_quote(&s)
        )
    })
}

/// Go's `%q` of a string, for the printable text a value carries.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `ScriptParser.ParseEvents` (`script.go:37-85`): events separated by an
/// empty line, each its command then its frames' symbols, reversed to
/// root first, each counted 1. Entry 55: an event the end of the text
/// cuts off is kept.
fn perf_script(bytes: &[u8]) -> Result<Vec<(Vec<String>, i64)>, String> {
    let lines: Vec<&[u8]> = bytes.split(|&b| b == b'\n').collect();
    let mut at = 0;
    let mut stacks = Stacks::default();
    while at < lines.len() {
        let line = lines[at];
        at += 1;
        let comm = match event_start(line) {
            Ok(comm) => comm,
            Err(_) if line.is_empty() && at >= lines.len() => break,
            Err(e) => return Err(e),
        };
        let mut frames: Vec<String> = Vec::new();
        while at < lines.len() {
            let line = lines[at];
            at += 1;
            if line.is_empty() {
                break;
            }
            let caps = frame_re()
                .captures(line)
                .ok_or_else(|| "reStackFrame mismatch".to_string())?;
            frames.push(text(caps.get(2).map(|m| m.as_bytes()).unwrap_or_default()));
        }
        frames.push(text(comm));
        frames.reverse();
        stacks.add(frames, 1);
    }
    Ok(stacks.order)
}

/// `JSONToProfile` (`convert.go:166-193`): the file's checks, its stacks,
/// its metadata.
fn json(bytes: &[u8]) -> Result<ConvertedType, String> {
    let f = pulsus_profiles_wire::flamebearer::parse_file(bytes)?;
    let stacks = pulsus_profiles_wire::flamebearer::stacks_of_levels(&f.names, &f.levels)
        .map_err(|e| format!("could not convert flameabearer to tree: {e}"))?;
    let mut merged = Stacks::default();
    for (frames, v) in stacks {
        merged.add(frames, v);
    }
    Ok(ConvertedType {
        display_name: f.name,
        spy_name: f.spy_name,
        sample_rate: f.sample_rate,
        units: f.units,
        stacks: merged.order,
    })
}

/// `PprofToProfile` (`convert.go:237-299`): one type per sample type; a
/// sample's stack its locations' lines, leaf first, reversed. Entry 54:
/// locations and functions are found by id.
fn pprof(bytes: &[u8]) -> Result<Vec<ConvertedType>, ConvertError> {
    use pulsus_profiles_wire::google_v1::{PprofDecodeError, decode_sized};
    let p = match decode_sized(bytes, MAX_PPROF_BYTES) {
        Ok((p, _)) => p,
        Err(PprofDecodeError::TooLarge { .. }) => return Err(ConvertError::TooLarge),
        Err(PprofDecodeError::Other(e)) => {
            return Err(ConvertError::Invalid(format!("parsing pprof: {e}")));
        }
    };
    let invalid = ConvertError::Invalid;
    let string = |i: i64| -> String {
        usize::try_from(i)
            .ok()
            .and_then(|i| p.string_table.get(i))
            .cloned()
            .unwrap_or_default()
    };
    let locations: std::collections::HashMap<u64, &pulsus_profiles_wire::google_v1::Location> =
        p.location.iter().map(|l| (l.id, l)).collect();
    let functions: std::collections::HashMap<u64, &pulsus_profiles_wire::google_v1::Function> =
        p.function.iter().map(|f| (f.id, f)).collect();
    let mut out = Vec::with_capacity(p.sample_type.len());
    for k in 0..p.sample_type.len() {
        let mut stacks = Stacks::default();
        for (i, sample) in p.sample.iter().enumerate() {
            let mut leaf_first: Vec<String> = Vec::new();
            for id in &sample.location_id {
                let loc = locations
                    .get(id)
                    .ok_or_else(|| invalid(format!("invalid location ID {id} in sample {i}")))?;
                if loc.line.is_empty() {
                    leaf_first.push(format!("{:x}", loc.address));
                    continue;
                }
                for line in &loc.line {
                    let f = functions.get(&line.function_id).ok_or_else(|| {
                        invalid(format!("invalid function ID {}", line.function_id))
                    })?;
                    leaf_first.push(string(f.name));
                }
            }
            let v = *sample.value.get(k).ok_or_else(|| {
                invalid(format!(
                    "invalid sampleType index {k} for sample {i} (len={})",
                    sample.value.len()
                ))
            })?;
            leaf_first.reverse();
            stacks.add(leaf_first, v);
        }
        let (sample_type, sample_unit) = profile_type(&p, k).map_err(invalid)?;
        let id = format!(":{sample_type}:{sample_unit}");
        let (units, sample_rate, display) = pulsus_profiles_wire::flamebearer::metadata(&id);
        out.push(ConvertedType {
            display_name: display.to_string(),
            spy_name: String::new(),
            sample_rate,
            units: units.to_string(),
            stacks: stacks.order,
        });
    }
    if out.is_empty() {
        return Err(invalid("no supported sample type found".to_string()));
    }
    Ok(out)
}

/// `getProfileType` (`convert.go:195-235`): sample type `k`'s type and
/// unit, after the period type's checks.
fn profile_type(
    p: &pulsus_profiles_wire::google_v1::Profile,
    k: usize,
) -> Result<(String, String), String> {
    let period = p.period_type.as_ref().ok_or("PeriodType is nil")?;
    let n = p.string_table.len() as i64;
    let get = |i: i64, what: String| -> Result<String, String> {
        if i < 0 || i >= n {
            return Err(format!("invalid {what}: {i}"));
        }
        Ok(p.string_table[i as usize].clone())
    };
    get(period.r#type, "PeriodType".to_string())?;
    get(period.unit, "PeriodUnit".to_string())?;
    let st = &p.sample_type[k];
    Ok((
        get(st.r#type, format!("SampleType[{k}]"))?,
        get(st.unit, format!("SampleUnit[{k}]"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::profiles::adhoc::fx;

    fn stack(frames: &[&str], v: i64) -> (Vec<String>, i64) {
        (frames.iter().map(|f| f.to_string()).collect(), v)
    }

    fn invalid(format: Format, name: &str, bytes: &[u8]) -> String {
        match convert(format, name, bytes) {
            Err(ConvertError::Invalid(e)) => e,
            other => panic!("{name}: {other:?}"),
        }
    }

    /// u1: the detection order.
    #[test]
    fn u1_detect() {
        let collapsed = b"main;a 10\n";
        assert_eq!(detect("a.json", collapsed), Ok(Format::Json));
        assert_eq!(detect("a.pprof", collapsed), Ok(Format::Pprof));
        assert_eq!(detect("a.collapsed", fx::F_S), Ok(Format::Collapsed));
        assert_eq!(detect("a.perf_script", collapsed), Ok(Format::PerfScript));
        assert_eq!(detect("perf.txt", fx::F_S), Ok(Format::PerfScript));
        assert_eq!(detect("perf", fx::F_S), Ok(Format::PerfScript));
        assert_eq!(detect("c.txt", fx::F_U_COLLAPSED), Ok(Format::Collapsed));
        assert_eq!(detect("c", fx::F_U_COLLAPSED), Ok(Format::Pprof));
        assert_eq!(detect("c", fx::F_J), Ok(Format::Json));
        assert_eq!(detect("x", &[0x1f, 0x8b, 0x08]), Ok(Format::Pprof));
        assert_eq!(detect("x", b"main;a\xa0b 1\n"), Ok(Format::Collapsed));
        assert_eq!(detect("x", b"m"), Err("profile is too short".to_string()));
        assert_eq!(detect("a.json", b"m"), Ok(Format::Json));
    }

    /// u2: the collapsed split and its texts.
    #[test]
    fn u2_collapsed() {
        let c = convert(Format::Collapsed, "c.txt", b"a;b 2\n\nmain;a 10").expect("converts");
        assert_eq!(c.types.len(), 1);
        let t = &c.types[0];
        assert_eq!(
            (
                t.display_name.as_str(),
                t.spy_name.as_str(),
                t.sample_rate,
                t.units.as_str()
            ),
            ("c.txt", "unknown", 100, "")
        );
        assert_eq!(
            t.stacks,
            vec![stack(&["a", "b"], 2), stack(&["main", "a"], 10)]
        );
        assert_eq!(
            invalid(Format::Collapsed, "c.txt", b"main;a x"),
            r#"unable to parse sample value: strconv.ParseUint: parsing "x": invalid syntax"#
        );
        assert_eq!(
            invalid(Format::Collapsed, "c.txt", b"main"),
            "unable to find stacktrace and value separator"
        );
    }

    /// u3: a perf script's frames reversed under the command; entry 55,
    /// the last event kept at the end of the text.
    #[test]
    fn u3_perf() {
        let s = convert(Format::PerfScript, "perf.txt", fx::F_S).expect("F-S");
        assert_eq!(
            s.types[0].stacks,
            vec![
                stack(&["java", "bar", "foo"], 1),
                stack(&["java", "foo"], 1)
            ]
        );
        let se = convert(Format::PerfScript, "p.txt", fx::F_SE).expect("F-SE");
        assert_eq!(
            se.types[0].stacks,
            vec![stack(&["java", "foo"], 1), stack(&["java", "bar"], 1)]
        );
    }

    /// u4: a pprof's types and stacks, zero values kept; entry 54, the
    /// locations and functions found by id; no period type refused.
    #[test]
    fn u4_pprof() {
        let p = convert(Format::Pprof, "p.pb.gz", &fx::f_p()).expect("F-P");
        assert_eq!(p.types.len(), 2);
        let (cpu, samples) = (&p.types[0], &p.types[1]);
        assert_eq!(
            (
                cpu.display_name.as_str(),
                cpu.spy_name.as_str(),
                cpu.sample_rate,
                cpu.units.as_str()
            ),
            ("cpu", "", 1_000_000_000, "samples")
        );
        assert_eq!(
            (
                samples.display_name.as_str(),
                samples.sample_rate,
                samples.units.as_str()
            ),
            ("samples", 100, "objects")
        );
        assert_eq!(
            cpu.stacks,
            vec![
                stack(&["main", "a"], 10),
                stack(&["main", "b"], 0),
                stack(&["main", "1f2e"], 2)
            ]
        );
        assert_eq!(
            samples.stacks,
            vec![
                stack(&["main", "a"], 1),
                stack(&["main", "b"], 4),
                stack(&["main", "1f2e"], 0)
            ]
        );
        let ids = convert(Format::Pprof, "ids", &fx::f_ids()).expect("F-IDS");
        assert_eq!(
            ids.types[0].stacks,
            vec![stack(&["main", "a"], 5), stack(&["main", "b"], 1)]
        );
        let f = convert(Format::Pprof, "f", &fx::f_fn_order()).expect("functions by id");
        assert_eq!(f.types[0].stacks, vec![stack(&["main", "a"], 1)]);
        assert_eq!(
            invalid(Format::Pprof, "n", &fx::f_no_period()),
            "PeriodType is nil"
        );
    }
}
