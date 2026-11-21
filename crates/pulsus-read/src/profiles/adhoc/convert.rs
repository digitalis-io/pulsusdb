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

/// The format of `bytes` uploaded as `name` (§4.1).
pub fn detect(_name: &str, _bytes: &[u8]) -> Result<Format, String> {
    Err(String::new())
}

/// Whether the text's first line is a perf script's event line.
pub fn is_perf_script(_bytes: &[u8]) -> bool {
    false
}

/// `bytes`, uploaded as `name`, converted as `format` (§4.2).
pub fn convert(_format: Format, _name: &str, _bytes: &[u8]) -> Result<Converted, ConvertError> {
    Err(ConvertError::Invalid(String::new()))
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
            (t.display_name.as_str(), t.spy_name.as_str(), t.sample_rate, t.units.as_str()),
            ("c.txt", "unknown", 100, "")
        );
        assert_eq!(t.stacks, vec![stack(&["a", "b"], 2), stack(&["main", "a"], 10)]);
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
            vec![stack(&["java", "bar", "foo"], 1), stack(&["java", "foo"], 1)]
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
            (cpu.display_name.as_str(), cpu.spy_name.as_str(), cpu.sample_rate, cpu.units.as_str()),
            ("cpu", "", 1_000_000_000, "samples")
        );
        assert_eq!(
            (samples.display_name.as_str(), samples.sample_rate, samples.units.as_str()),
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
