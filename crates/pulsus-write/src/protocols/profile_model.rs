//! The pprof model: a push's profiles become the rows of
//! [`ParsedProfileLanding`], validated as the reference validates them. Pure
//! functions, no I/O, no clock and no randomness: the receive time is a
//! parameter and an absent profile id stays `None` for the writer to mint.
//!
//! [`parse_profiles`] runs nine steps per profile, in this order, and the
//! first refusal refuses the whole push:
//!
//! 1. a missing `service_name` becomes `unknown_service`; labels are sorted;
//! 2. the profile checks (`pkg/validation/validate.go:323-445`), with the
//!    stack-depth and symbol-length caps;
//! 3. language detection (`pkg/distributor/model/push.go:57-80`,
//!    `pkg/pprof/pprof.go:1245-1269`);
//! 4. for Go, the type-parameter fix and the truncated-stack repair
//!    (`pkg/pprof/fix_go_profile.go`, `pkg/pprof/fix_go_truncated.go`);
//! 5. normalisation (`pkg/pprof/pprof.go:489-523`);
//! 6. reference cleanup (`pkg/pprof/pprof.go:1393-1560`, `:551-567`);
//! 7. the split by sample labels, the default relabel rules and the label
//!    checks (`pkg/model/pprofsplit/pprof_split.go:26-120`,
//!    `pkg/validation/relabeling.go:13-56`, `pkg/validation/validate.go:132-204`);
//! 8. one label set per sample type (`pkg/phlaredb/labels/labels.go:14-69`);
//! 9. the rows.
//!
//! Every reference limit is a constant here, not a setting.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;

use prost::Message;
use pulsus_model::{LabelSet, raw_cityhash64, stream_fingerprint};
use pulsus_profiles_wire::google_v1::{Function, Mapping, Profile};
use xxhash_rust::xxh64::xxh64;

use crate::ingest::profiles::{
    LandingEntry, LandingLocation, LandingSample, LandingSeries, LandingStack,
    ParsedProfileLanding, ProfileInput, ProfileRefusal,
};

/// `validation.max-profile-size-bytes` (`pkg/validation/limits.go:196`).
pub const MAX_PROFILE_SIZE_BYTES: u64 = 4 * 1024 * 1024;
/// `validation.max-profile-stacktrace-samples`.
pub const MAX_PROFILE_SAMPLES: usize = 16_000;
/// `validation.max-profile-stacktrace-sample-labels`.
pub const MAX_SAMPLE_LABELS: usize = 100;
/// `validation.max-profile-stacktrace-depth`: deeper stacks keep their
/// root-side frames.
pub const MAX_STACK_DEPTH: usize = 1_000;
/// `validation.max-profile-symbol-value-length`: longer strings keep their
/// last bytes.
pub const MAX_SYMBOL_BYTES: usize = 65_535;
/// `validation.max-label-names-per-series` (`pkg/validation/limits.go:170`).
pub const MAX_LABEL_NAMES: usize = 30;
/// `validation.max-length-label-name`.
pub const MAX_LABEL_NAME_BYTES: usize = 1_024;
/// `validation.max-length-label-value`.
pub const MAX_LABEL_VALUE_BYTES: usize = 2_048;
/// The ingestion window's start, before the receive time.
pub const REJECT_OLDER_THAN_MS: i64 = 3_600_000;
/// The ingestion window's end, after the receive time.
pub const REJECT_NEWER_THAN_MS: i64 = 600_000;
/// The `service_name` a push without one gets
/// (`pkg/distributor/distributor.go:592-596`).
pub const SERVICE_NAME_FALLBACK: &str = "unknown_service";

const SERVICE_NAME: &str = "service_name";
const METRIC_NAME: &str = "__name__";
const SPAN_ID: &str = "span_id";
const TRACE_ID: &str = "trace_id";
const PROFILE_ID: &str = "profile_id";
/// `pkg/model/labels.go:57`.
const SPY_LABEL: &str = "pyroscope_spy";

/// Decodes one pprof body: gzipped when it starts `1f 8b`, else the raw
/// message. Output over `limit` bytes is refused
/// (`pkg/pprof/pprof.go:96-129`); receivers pass [`MAX_PROFILE_SIZE_BYTES`].
pub fn decode_pprof(bytes: &[u8], limit: u64) -> Result<Profile, String> {
    let too_big = || format!("decompressed size exceeds maximum allowed size of {limit} bytes");
    if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut buf = Vec::new();
        flate2::read::MultiGzDecoder::new(bytes)
            .take(limit.saturating_add(1))
            .read_to_end(&mut buf)
            .map_err(|e| format!("copy to buffer: {e}"))?;
        if buf.len() as u64 > limit {
            return Err(too_big());
        }
        Profile::decode(buf.as_slice()).map_err(|e| e.to_string())
    } else {
        if bytes.len() as u64 > limit {
            return Err(too_big());
        }
        Profile::decode(bytes).map_err(|e| e.to_string())
    }
}

/// Turns a push's profiles into its rows. `now_ms` is the receive time: the
/// ingestion window is measured from it, and a profile without a time takes
/// it. Any refusal refuses the whole push, and nothing is returned.
pub fn parse_profiles(
    inputs: Vec<ProfileInput>,
    now_ms: i64,
) -> Result<ParsedProfileLanding, ProfileRefusal> {
    let mut push = Push::default();
    for (i, input) in inputs.into_iter().enumerate() {
        push.add(i as u32, input, now_ms)?;
    }
    Ok(push.out)
}

fn refuse(message: String) -> ProfileRefusal {
    ProfileRefusal { message }
}

/// A string-table entry; an index outside the table reads as `""`, as the
/// reference's cleanup maps it to index 0 (`pkg/pprof/pprof.go:1422-1434`).
fn st(table: &[String], i: i64) -> &str {
    usize::try_from(i)
        .ok()
        .and_then(|i| table.get(i))
        .map_or("", String::as_str)
}

/// `compose128` (`crates/pulsus-model/src/fingerprint.rs:84`) as the bare
/// integer the id columns hold: `cityHash64` high, `xxHash64` low.
fn id128(buf: &[u8]) -> u128 {
    ((raw_cityhash64(buf) as u128) << 64) | (xxh64(buf, 0) as u128)
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

type Labels = Vec<(String, String)>;

fn get<'a>(ls: &'a [(String, String)], name: &str) -> &'a str {
    ls.iter()
        .find(|(k, _)| k == name)
        .map_or("", |(_, v)| v.as_str())
}

/// The reference's `LabelsBuilder.Set`: an empty value deletes the label.
fn set(ls: &mut Labels, name: &str, value: &str) {
    ls.retain(|(k, _)| k != name);
    if !value.is_empty() {
        ls.push((name.to_owned(), value.to_owned()));
    }
}

fn sort_labels(ls: &mut Labels) {
    ls.sort_by(|a, b| a.0.cmp(&b.0));
}

/// `LabelPairsString` (`pkg/model/labels.go:353-367`).
fn label_text(ls: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in ls.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        go_quote(&mut out, v);
    }
    out.push('}');
    out
}

/// Go's `strconv.Quote`. Control characters are escaped; every other
/// character is kept as it is.
fn go_quote(out: &mut String, s: &str) {
    use std::fmt::Write;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go's `Time.String()` for a UTC time in milliseconds
/// (`pkg/util/time.go:30`): `2027-01-15 07:00:00.5 +0000 UTC`.
fn go_time_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut out = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    );
    if milli != 0 {
        let frac = format!("{milli:03}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push_str(" +0000 UTC");
    out
}

/// Days since 1970-01-01 to a proleptic Gregorian date.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// The label checks (`pkg/validation/validate.go:132-204`), on a sorted set.
fn validate_labels(ls: &[(String, String)]) -> Result<(), ProfileRefusal> {
    if ls.len() > MAX_LABEL_NAMES {
        return Err(refuse(format!(
            "profile series '{}' has {} label names; limit {MAX_LABEL_NAMES}",
            label_text(ls),
            ls.len()
        )));
    }
    if get(ls, METRIC_NAME).is_empty() {
        return Err(refuse(format!(
            "invalid labels '{}' with error: invalid metric name",
            label_text(ls)
        )));
    }
    let mut last: Option<&str> = None;
    for (name, value) in ls {
        if name.len() > MAX_LABEL_NAME_BYTES {
            return Err(refuse(format!(
                "profile with labels '{}' has label name too long: '{name}'",
                label_text(ls)
            )));
        }
        if value.len() > MAX_LABEL_VALUE_BYTES {
            return Err(refuse(format!(
                "profile with labels '{}' has label value too long: '{value}'",
                label_text(ls)
            )));
        }
        if name.is_empty() {
            return Err(refuse(format!(
                "invalid labels '{}' with error: invalid label name '{name}'",
                label_text(ls)
            )));
        }
        if last == Some(name.as_str()) {
            return Err(refuse(format!(
                "profile with labels '{}' has duplicate label name: '{name}'",
                label_text(ls)
            )));
        }
        last = Some(name);
    }
    Ok(())
}

/// `godeltaprof_(.*)`, anchored, `.` matching any character.
fn godeltaprof(v: &str) -> Option<&str> {
    v.strip_prefix("godeltaprof_")
}

/// The default relabel rules (`pkg/validation/relabeling.go:13-56`), with
/// Prometheus `replace` semantics: no match changes nothing, and an empty
/// result deletes the target.
fn relabel(ls: &mut Labels) {
    if godeltaprof(get(ls, METRIC_NAME)).is_some() {
        let whole = get(ls, METRIC_NAME).to_owned();
        set(ls, "__name_replaced__", &whole);
    }
    if godeltaprof(get(ls, "__name_replaced__")).is_some() {
        set(ls, "__delta__", "false");
    }
    if let Some(rest) = godeltaprof(get(ls, METRIC_NAME)) {
        let rest = rest.to_owned();
        set(ls, METRIC_NAME, &rest);
    }
    let wall_cpu = |ls: &Labels| get(ls, METRIC_NAME) == "wall" && get(ls, "__type__") == "cpu";
    if wall_cpu(ls) {
        set(ls, "__name_replaced__", "wall");
    }
    if wall_cpu(ls) {
        set(ls, METRIC_NAME, "process_cpu");
    }
}

/// 16 hex digits read big-endian; `None` when malformed.
fn parse_span_id(s: &str) -> Option<u64> {
    let mut out = [0u8; 8];
    decode_hex(s, &mut out)?;
    Some(u64::from_be_bytes(out))
}

/// 32 hex digits; `None` when malformed.
fn parse_trace_id(s: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    decode_hex(s, &mut out)?;
    Some(out)
}

fn decode_hex(s: &str, out: &mut [u8]) -> Option<()> {
    let b = s.as_bytes();
    if b.len() != out.len() * 2 {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    for (i, o) in out.iter_mut().enumerate() {
        *o = (digit(b[2 * i])? << 4) | digit(b[2 * i + 1])?;
    }
    Some(())
}

// ---------------------------------------------------------------------------
// Step 2: the profile checks
// ---------------------------------------------------------------------------

/// `ValidateProfile` (`pkg/validation/validate.go:323-445`). Returns the
/// profile's time in nanoseconds and whether it was defaulted.
fn validate_profile(
    p: &mut Profile,
    decompressed_bytes: u64,
    text: &str,
    now_ms: i64,
) -> Result<(i64, bool), ProfileRefusal> {
    if p.sample_type.is_empty() {
        return Err(refuse("empty profile".to_owned()));
    }
    let time = if p.time_nanos > 0 {
        let t = p.time_nanos / 1_000_000;
        let from = now_ms - REJECT_OLDER_THAN_MS;
        let to = now_ms + REJECT_NEWER_THAN_MS;
        if !(t > from && t < to) {
            return Err(refuse(format!(
                "profile with labels '{text}' is outside of ingestion window (profile timestamp: {}, \
                 the ingestion window starts at {} and ends at {})",
                go_time_millis(t),
                go_time_millis(from),
                go_time_millis(to)
            )));
        }
        (p.time_nanos, false)
    } else {
        (now_ms.saturating_mul(1_000_000), true)
    };
    if decompressed_bytes > MAX_PROFILE_SIZE_BYTES {
        return Err(refuse(format!(
            "the profile with labels '{text}' exceeds the size limit (max_profile_size_byte, \
             actual: {decompressed_bytes}, limit: {MAX_PROFILE_SIZE_BYTES})"
        )));
    }
    if p.sample.len() > MAX_PROFILE_SAMPLES {
        return Err(refuse(format!(
            "the profile with labels '{text}' exceeds the samples count limit \
             (max_profile_stacktrace_samples, actual: {}, limit: {MAX_PROFILE_SAMPLES})",
            p.sample.len()
        )));
    }
    for s in &mut p.sample {
        if s.location_id.len() > MAX_STACK_DEPTH {
            // `location_id[0]` is the leaf: the deepest frames go.
            s.location_id.drain(..s.location_id.len() - MAX_STACK_DEPTH);
        }
        if s.label.len() > MAX_SAMPLE_LABELS {
            return Err(refuse(format!(
                "the profile with labels '{text}' exceeds the sample labels limit \
                 (max_profile_stacktrace_sample_labels, actual: {}, limit: {MAX_SAMPLE_LABELS})",
                s.label.len()
            )));
        }
    }
    for s in &mut p.string_table {
        if s.len() > MAX_SYMBOL_BYTES {
            let mut start = s.len() - MAX_SYMBOL_BYTES;
            while !s.is_char_boundary(start) {
                start += 1;
            }
            s.drain(..start);
        }
    }
    if p.location.iter().any(|l| l.id == 0) {
        return Err(refuse("location id is 0".to_owned()));
    }
    if p.function.iter().any(|f| f.id == 0) {
        return Err(refuse("function id is 0".to_owned()));
    }
    if p.sample
        .iter()
        .any(|s| s.value.len() != p.sample_type.len())
    {
        return Err(refuse("sample value length mismatch".to_owned()));
    }
    validate_string_table_access(p)?;
    if p.sample_type
        .iter()
        .any(|vt| st(&p.string_table, vt.r#type).contains('-'))
    {
        return Err(refuse("sample type contains -".to_owned()));
    }
    Ok(time)
}

/// `validateStringTableAccess`: an index at or above the table's length is
/// refused; a negative one is not (it reads as `""`).
fn validate_string_table_access(p: &Profile) -> Result<(), ProfileRefusal> {
    let table = &p.string_table;
    if table.first().is_none_or(|s| !s.is_empty()) {
        return Err(refuse("string 0 should be empty string".to_owned()));
    }
    let len = table.len() as i64;
    let out = |i: i64| i >= len;
    let fail = |m: &str| Err(refuse(m.to_owned()));
    if p.sample_type.iter().any(|vt| out(vt.r#type)) {
        return fail("sample type type string index out of range");
    }
    if p.sample
        .iter()
        .flat_map(|s| &s.label)
        .any(|l| out(l.str) || out(l.key))
    {
        return fail("sample label string index out of range");
    }
    for f in &p.function {
        if out(f.name) {
            return fail("function name string index out of range");
        }
        if out(f.system_name) {
            return fail("function system name index string out of range");
        }
        if out(f.filename) {
            return fail("function file name string index out of range");
        }
    }
    for m in &p.mapping {
        if out(m.filename) {
            return fail("mapping file name string index out of range");
        }
        if out(m.build_id) {
            return fail("mapping build id string index out of range");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Steps 3-4: language and the Go fix-ups
// ---------------------------------------------------------------------------

/// The language matchers, in the reference's order
/// (`pkg/pprof/pprof.go:1245-1256`).
const LANGUAGE_MATCHERS: &[(&str, &[&str])] = &[
    ("go", &[".go", "/usr/local/go/"]),
    ("java", &["java/", "sun/"]),
    ("ruby", &[".rb", "gems/"]),
    ("nodejs", &["./node_modules/", ".js"]),
    ("dotnet", &["System.", "Microsoft."]),
    ("python", &[".py"]),
    ("rust", &["main.rs", "core.rs"]),
];

/// The spy label's language (`pkg/distributor/model/push.go:57-80`), else
/// the first string-table entry that starts or ends with a pattern.
fn language(labels: &[(String, String)], p: &Profile) -> &'static str {
    let spy = match get(labels, SPY_LABEL) {
        "dotnetspy" => "dotnet",
        "gospy" => "go",
        "phpspy" => "php",
        "pyspy" => "python",
        "rbspy" => "ruby",
        "nodespy" => "nodejs",
        "javaspy" | "grafana-agent.java" => "java",
        "pyroscope-rs" => "rust",
        _ => "",
    };
    if !spy.is_empty() {
        return spy;
    }
    for symbol in &p.string_table {
        for (lang, patterns) in LANGUAGE_MATCHERS {
            if patterns
                .iter()
                .any(|pat| symbol.starts_with(pat) || symbol.ends_with(pat))
            {
                return lang;
            }
        }
    }
    "unknown"
}

/// `FixGoProfile` (`pkg/pprof/fix_go_profile.go:16-25`), less the
/// self-merge: rows that become equal collapse in the id grouping anyway.
fn fix_go_profile(p: &mut Profile) {
    for s in &mut p.string_table {
        if let Some(fixed) = drop_go_type_parameters(s) {
            *s = fixed;
        }
    }
    if potential_truncated_go_stacktraces(p) {
        repair_go_truncated_stacktraces(p);
    }
}

const GO_SHAPE_PREFIX: &str = "[go.shape.";

/// `dropGoTypeParameters` (`pkg/pprof/fix_go_profile.go:64-109`): each
/// `[go.shape.`… section up to its matching `]` is removed. `None` when the
/// input has none.
fn drop_go_type_parameters(input: &str) -> Option<String> {
    if !input.contains(GO_SHAPE_PREFIX) {
        return None;
    }
    let b = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < b.len() {
        let Some(start) = input[i..].find(GO_SHAPE_PREFIX) else {
            out.push_str(&input[i..]);
            break;
        };
        out.push_str(&input[i..i + start]);
        let mut depth = 0i64;
        let mut j = i + start;
        while j < b.len() {
            if b[j] == b'[' {
                depth += 1;
            } else if b[j] == b']' {
                depth -= 1;
                if depth == 0 {
                    i = j + 1;
                    break;
                }
            }
            j += 1;
        }
        if depth != 0 {
            // No matching bracket: the rest is kept as it is.
            out.push_str(&input[i..]);
            break;
        }
    }
    Some(out)
}

const MIN_GROUP_SIZE: isize = 2;
const MAX_RECURSIVE_DEPTH: usize = 56;
const TOKENS: usize = 8;
const TOKEN_LEN: usize = 16;
const SUFFIX_LEN: usize = TOKENS + TOKEN_LEN;
const TOKEN_BYTES_LEN: usize = TOKEN_LEN * 8;
const SUFFIX_BYTES_LEN: usize = SUFFIX_LEN * 8;

/// `PotentialTruncatedGoStacktraces` (`pkg/pprof/fix_go_truncated.go:26-56`).
fn potential_truncated_go_stacktraces(p: &Profile) -> bool {
    let has = |names: &[&str]| {
        p.sample_type
            .iter()
            .any(|vt| names.contains(&st(&p.string_table, vt.r#type)))
    };
    let (min_depth, max_depth) = if has(&[
        "alloc_objects",
        "alloc_space",
        "inuse_objects",
        "inuse_space",
    ]) {
        (32 - 4, 33)
    } else if has(&["cpu"]) {
        (64 - 4, 0)
    } else {
        return false;
    };
    let mut deep_enough = false;
    for s in &p.sample {
        if s.location_id.len() >= min_depth {
            deep_enough = true;
        }
        if max_depth != 0 && s.location_id.len() >= max_depth {
            return false;
        }
    }
    deep_enough
}

/// The last `n` location ids as little-endian bytes, the reference's
/// in-memory layout, so groups order as its byte comparison orders them.
fn tail_bytes(ids: &[u64], n: usize) -> Vec<u8> {
    ids[ids.len() - n..]
        .iter()
        .flat_map(|id| id.to_le_bytes())
        .collect()
}

#[derive(Clone, Copy, Default)]
struct Group {
    gid: u32,
    off: u32,
}

/// `RepairGoTruncatedStacktraces` (`pkg/pprof/fix_go_truncated.go:99-231`):
/// a stack that overlaps a longer one by a whole token at its root end is
/// taken as truncated, and the missing root-side frames are copied from the
/// longer one.
fn repair_go_truncated_stacktraces(p: &mut Profile) {
    // `split`: the samples of at least `SUFFIX_LEN` frames, ordered by
    // their root-side suffix, and the first index of each group.
    let mut order: Vec<usize> = (0..p.sample.len()).collect();
    let suffixes: Vec<Option<Vec<u8>>> = p
        .sample
        .iter()
        .map(|s| {
            (s.location_id.len() >= SUFFIX_LEN).then(|| tail_bytes(&s.location_id, SUFFIX_LEN))
        })
        .collect();
    order.sort_by(|&a, &b| match (&suffixes[a], &suffixes[b]) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(y),
    });
    let o = order.partition_point(|&i| suffixes[i].is_none());
    let samples = &order[o..];
    if samples.is_empty() {
        return;
    }
    let mut groups: Vec<usize> = Vec::new();
    let mut prev: Option<&Vec<u8>> = None;
    for (i, &s) in samples.iter().enumerate() {
        let cur = suffixes[s].as_ref();
        if cur != prev {
            groups.push(i);
            prev = cur;
        }
    }
    // The reference's group end: the next group's start, or the number of
    // groups for the last one.
    let group_end = |i: usize| {
        if i + 1 < groups.len() {
            groups[i + 1]
        } else {
            groups.len()
        }
    };

    let mut m: HashMap<Vec<u8>, Group> = HashMap::with_capacity(groups.len() / 2);
    for (i, &g) in groups.iter().enumerate() {
        if (group_end(i) as isize) - (g as isize) < MIN_GROUP_SIZE - 1 {
            continue;
        }
        let s = suffixes[samples[g]]
            .as_ref()
            .expect("grouped samples have a suffix");
        for j in 1..=TOKENS as u32 {
            let hi = SUFFIX_BYTES_LEN - j as usize * TOKENS;
            let lo = hi - TOKEN_BYTES_LEN;
            let k = &s[lo..hi];
            let replace = m.get(k).is_none_or(|c| j > c.off);
            if replace {
                m.insert(
                    k.to_vec(),
                    Group {
                        gid: i as u32 + 1,
                        off: j,
                    },
                );
            }
        }
    }

    let mut d = vec![Group::default(); groups.len()];
    for (i, &g) in groups.iter().enumerate() {
        let t = tail_bytes(&p.sample[samples[g]].location_id, TOKEN_LEN);
        match m.get(&t) {
            Some(c) if c.gid - 1 != i as u32 => d[i] = *c,
            _ => {}
        }
    }

    for i in 0..groups.len() {
        let g = groups[i];
        let mut c = d[i];
        let mut off: u32 = 0;
        let mut j = 0;
        while c.gid > 0 && c.off > 0 {
            off += c.off;
            let n = d[(c.gid - 1) as usize];
            if n.gid == 0 || c.off == 0 {
                break;
            }
            c = n;
            j += 1;
            if j == MAX_RECURSIVE_DEPTH {
                return;
            }
        }
        if off == 0 {
            continue;
        }
        let appx = &p.sample[samples[groups[(c.gid - 1) as usize]]].location_id;
        if off as usize >= appx.len() {
            continue;
        }
        let missing = appx[appx.len() - off as usize..].to_vec();
        for &s in &samples[g..group_end(i)] {
            p.sample[s].location_id.extend_from_slice(&missing);
        }
    }
}

// ---------------------------------------------------------------------------
// Steps 5-6: normalisation and references
// ---------------------------------------------------------------------------

/// Labels without a string value go; a sample with a negative value, or
/// with no positive one, goes.
fn normalise(p: &mut Profile) {
    for s in &mut p.sample {
        s.label.retain(|l| l.str != 0);
    }
    p.sample
        .retain(|s| !s.value.iter().any(|&v| v < 0) && s.value.iter().any(|&v| v > 0));
}

/// One line of a resolved location.
struct ResolvedLine<'a> {
    name: &'a str,
    system_name: &'a str,
    filename: &'a str,
    start_line: i64,
    line: i64,
}

/// A location after the reference cleanup and `clearAddresses`.
struct ResolvedLocation<'a> {
    file: &'a str,
    build_id: &'a str,
    start: u64,
    limit: u64,
    offset: u64,
    address: u64,
    lines: Vec<ResolvedLine<'a>>,
}

impl ResolvedLocation<'_> {
    /// The location's content id: every column the row stores.
    fn id(&self) -> u128 {
        let mut buf = Vec::with_capacity(64 + self.lines.len() * 48);
        let s = |buf: &mut Vec<u8>, v: &str| {
            buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        };
        s(&mut buf, self.file);
        s(&mut buf, self.build_id);
        for n in [self.start, self.limit, self.offset, self.address] {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        buf.extend_from_slice(&(self.lines.len() as u64).to_le_bytes());
        for l in &self.lines {
            s(&mut buf, l.name);
            s(&mut buf, l.system_name);
            s(&mut buf, l.filename);
            buf.extend_from_slice(&l.start_line.to_le_bytes());
            buf.extend_from_slice(&l.line.to_le_bytes());
        }
        id128(&buf)
    }
}

/// The surviving locations, and for each surviving sample its index and its
/// locations (leaf first) as indices into them.
struct Resolved<'a> {
    locations: Vec<ResolvedLocation<'a>>,
    samples: Vec<(usize, Vec<usize>)>,
}

/// `sanitizeProfile`'s reference rules (`pkg/pprof/pprof.go:1393-1560`) and
/// `clearAddresses` (`:551-567`). A later duplicate id wins, as the
/// reference's renumbering map overwrites.
fn resolve(p: &Profile) -> Resolved<'_> {
    let table = &p.string_table;
    let mappings: HashMap<u64, &Mapping> = p.mapping.iter().map(|m| (m.id, m)).collect();
    let functions: HashMap<u64, &Function> = p.function.iter().map(|f| (f.id, f)).collect();
    let mut locations = Vec::with_capacity(p.location.len());
    let mut by_id: HashMap<u64, usize> = HashMap::with_capacity(p.location.len());
    'loc: for l in &p.location {
        if l.line.is_empty() && l.address == 0 {
            continue;
        }
        let mapping = if l.mapping_id == 0 {
            None
        } else {
            match mappings.get(&l.mapping_id) {
                Some(m) => Some(*m),
                None => continue,
            }
        };
        let mut lines = Vec::with_capacity(l.line.len());
        for line in &l.line {
            let Some(f) = functions.get(&line.function_id) else {
                continue 'loc;
            };
            lines.push(ResolvedLine {
                name: st(table, f.name),
                system_name: st(table, f.system_name),
                filename: st(table, f.filename),
                start_line: f.start_line,
                line: line.line,
            });
        }
        let resolved = match mapping {
            None => ResolvedLocation {
                file: "",
                build_id: "",
                start: 0,
                limit: 0,
                offset: 0,
                address: l.address,
                lines,
            },
            Some(m) if m.has_functions => ResolvedLocation {
                file: st(table, m.filename),
                build_id: st(table, m.build_id),
                start: 0,
                limit: 0,
                offset: 0,
                address: 0,
                lines,
            },
            Some(m) => ResolvedLocation {
                file: st(table, m.filename),
                build_id: st(table, m.build_id),
                start: m.memory_start,
                limit: m.memory_limit,
                offset: m.file_offset,
                address: l.address,
                lines,
            },
        };
        by_id.insert(l.id, locations.len());
        locations.push(resolved);
    }
    let samples = p
        .sample
        .iter()
        .enumerate()
        .filter_map(|(i, s)| {
            let locs: Option<Vec<usize>> = s
                .location_id
                .iter()
                .map(|id| by_id.get(id).copied())
                .collect();
            locs.map(|locs| (i, locs))
        })
        .collect();
    Resolved { locations, samples }
}

// ---------------------------------------------------------------------------
// Steps 7-9: the split, the label sets and the rows
// ---------------------------------------------------------------------------

/// One series group: its label set and its samples, as indices into
/// [`Resolved::samples`] with each sample's span and trace id.
struct SampleGroup {
    labels: Labels,
    members: Vec<(usize, u64, [u8; 16])>,
}

#[derive(Default)]
struct Push {
    out: ParsedProfileLanding,
    strings: HashSet<Arc<str>>,
    stacks: HashSet<u128>,
    locations: HashSet<u128>,
}

impl Push {
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(a) = self.strings.get(s) {
            return Arc::clone(a);
        }
        let a: Arc<str> = Arc::from(s);
        self.strings.insert(Arc::clone(&a));
        a
    }

    fn add(&mut self, input: u32, src: ProfileInput, now_ms: i64) -> Result<(), ProfileRefusal> {
        let ProfileInput {
            labels,
            mut profile,
            decompressed_bytes,
            id,
            annotations,
        } = src;

        // 1. service name, then sort.
        let mut labels = labels;
        if get(&labels, SERVICE_NAME).is_empty() {
            labels.retain(|(k, _)| k != SERVICE_NAME);
            labels.push((SERVICE_NAME.to_owned(), SERVICE_NAME_FALLBACK.to_owned()));
        }
        sort_labels(&mut labels);
        let text = label_text(&labels);

        // 2. profile checks.
        let (timestamp_ns, time_defaulted) =
            validate_profile(&mut profile, decompressed_bytes, &text, now_ms)?;

        // 3-4. language and the Go fix-ups.
        if language(&labels, &profile) == "go" {
            fix_go_profile(&mut profile);
        }

        // 5-6. normalise, references.
        normalise(&mut profile);
        let resolved = resolve(&profile);
        let table = &profile.string_table;

        // 7. split. The builder's base drops every label given an empty
        // value (`LabelsBuilder.Reset`).
        let empty: HashSet<&str> = labels
            .iter()
            .filter(|(_, v)| v.is_empty())
            .map(|(k, _)| k.as_str())
            .collect();
        let base: Labels = labels
            .iter()
            .filter(|(k, _)| !empty.contains(k.as_str()))
            .cloned()
            .collect();
        let mut groups: Vec<SampleGroup> = Vec::new();
        let mut group_of: HashMap<Labels, usize> = HashMap::new();
        for (ri, (si, _)) in resolved.samples.iter().enumerate() {
            let sample = &profile.sample[*si];
            let mut ls = base.clone();
            let mut span: Option<u64> = None;
            let mut trace: Option<[u8; 16]> = None;
            for l in &sample.label {
                let mut name = st(table, l.key);
                if name == PROFILE_ID {
                    name = SPAN_ID;
                }
                let value = st(table, l.str);
                if name == SPAN_ID {
                    span = span.or_else(|| parse_span_id(value));
                    continue;
                }
                if name == TRACE_ID {
                    trace = trace.or_else(|| parse_trace_id(value));
                    continue;
                }
                if l.str <= 0 || !get(&ls, name).is_empty() {
                    continue;
                }
                set(&mut ls, name, value);
            }
            relabel(&mut ls);
            sort_labels(&mut ls);
            let member = (ri, span.unwrap_or(0), trace.unwrap_or([0; 16]));
            match group_of.get(&ls) {
                Some(&g) => groups[g].members.push(member),
                None => {
                    group_of.insert(ls.clone(), groups.len());
                    groups.push(SampleGroup {
                        labels: ls,
                        members: vec![member],
                    });
                }
            }
        }

        // 8-9. per group, per sample type.
        let (period_type, period_unit) = match &profile.period_type {
            Some(vt) => (st(table, vt.r#type), st(table, vt.unit)),
            None => ("", ""),
        };
        let annotation_keys: Vec<String> = annotations.iter().map(|(k, _)| k.clone()).collect();
        let annotation_values: Vec<String> = annotations.iter().map(|(_, v)| v.clone()).collect();
        let mut location_ids: Vec<Option<u128>> = vec![None; resolved.locations.len()];
        let mut stack_ids: Vec<Option<u128>> = vec![None; resolved.samples.len()];
        for group in &groups {
            validate_labels(&group.labels)?;
            let mut common: Labels = group.labels.clone();
            common.retain(|(k, _)| k != "__delta__" && k != "__order__");
            let metric_name = get(&group.labels, METRIC_NAME).to_owned();
            let service = get(&group.labels, SERVICE_NAME).to_owned();
            if !service.is_empty() {
                set(&mut common, "__service_name__", &service);
            }
            if profile.period_type.is_some() {
                set(&mut common, "__period_type__", period_type);
                set(&mut common, "__period_unit__", period_unit);
            }
            for (ti, vt) in profile.sample_type.iter().enumerate() {
                let members: Vec<&(usize, u64, [u8; 16])> = group
                    .members
                    .iter()
                    .filter(|(ri, _, _)| profile.sample[resolved.samples[*ri].0].value[ti] != 0)
                    .collect();
                if members.is_empty() {
                    continue;
                }
                let sample_type = st(table, vt.r#type);
                let sample_unit = st(table, vt.unit);
                let type_id = format!(
                    "{metric_name}:{sample_type}:{sample_unit}:{period_type}:{period_unit}"
                );
                let mut set_labels = common.clone();
                set(&mut set_labels, "__type__", sample_type);
                set(&mut set_labels, "__unit__", sample_unit);
                set(&mut set_labels, "__profile_type__", &type_id);
                let label_set = LabelSet::from_verbatim(set_labels);

                let series = self.out.series.len() as u32;
                let entry = self.out.entries.len() as u32;
                let type_id = self.intern(&type_id);
                let service = self.intern(label_set.service());
                self.out.series.push(LandingSeries {
                    type_id,
                    service,
                    fingerprint: stream_fingerprint(&label_set),
                    timestamp_ns,
                    time_defaulted,
                    labels: label_set.to_canonical_json(),
                });
                let mut total: i64 = 0;
                let mut rows: HashMap<(u128, u64, [u8; 16]), usize> = HashMap::new();
                for &&(ri, span_id, trace_id) in &members {
                    let value = profile.sample[resolved.samples[ri].0].value[ti];
                    total = total.wrapping_add(value);
                    let stack_id = match stack_ids[ri] {
                        Some(id) => id,
                        None => {
                            let id = self.stack(&resolved, ri, &mut location_ids);
                            stack_ids[ri] = Some(id);
                            id
                        }
                    };
                    match rows.get(&(stack_id, span_id, trace_id)) {
                        Some(&at) => {
                            let row = &mut self.out.samples[at];
                            row.value = row.value.wrapping_add(value);
                        }
                        None => {
                            rows.insert((stack_id, span_id, trace_id), self.out.samples.len());
                            self.out.samples.push(LandingSample {
                                entry,
                                stack_id,
                                value,
                                span_id,
                                trace_id,
                            });
                        }
                    }
                }
                self.out.entries.push(LandingEntry {
                    series,
                    input,
                    profile_id: id,
                    total,
                    duration_ns: profile.duration_nanos,
                    annotation_keys: annotation_keys.clone(),
                    annotation_values: annotation_values.clone(),
                });
            }
        }
        Ok(())
    }

    /// The stack id of resolved sample `ri`; the first time a stack is seen
    /// in the push, its row and any new location rows are added.
    fn stack(
        &mut self,
        resolved: &Resolved<'_>,
        ri: usize,
        location_ids: &mut [Option<u128>],
    ) -> u128 {
        let locs = &resolved.samples[ri].1;
        let ids: Vec<u128> = locs
            .iter()
            .map(|&li| *location_ids[li].get_or_insert_with(|| resolved.locations[li].id()))
            .collect();
        let mut buf = Vec::with_capacity(ids.len() * 16);
        for id in &ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        let stack_id = id128(&buf);
        if !self.stacks.insert(stack_id) {
            return stack_id;
        }
        let mut names = Vec::new();
        let mut node_ids = Vec::new();
        let mut parent: u128 = 0;
        for &li in locs.iter().rev() {
            for line in resolved.locations[li].lines.iter().rev() {
                let mut nb = Vec::with_capacity(16 + line.name.len());
                nb.extend_from_slice(&parent.to_le_bytes());
                nb.extend_from_slice(line.name.as_bytes());
                parent = id128(&nb);
                node_ids.push(parent);
                names.push(self.intern(line.name));
            }
        }
        for (&li, &location_id) in locs.iter().zip(&ids) {
            if !self.locations.insert(location_id) {
                continue;
            }
            let loc = &resolved.locations[li];
            let row = LandingLocation {
                location_id,
                mapping_file: self.intern(loc.file),
                mapping_build_id: self.intern(loc.build_id),
                mapping_start: loc.start,
                mapping_limit: loc.limit,
                mapping_offset: loc.offset,
                address: loc.address,
                line_functions: loc.lines.iter().map(|l| self.intern(l.name)).collect(),
                line_system_names: loc
                    .lines
                    .iter()
                    .map(|l| self.intern(l.system_name))
                    .collect(),
                line_files: loc.lines.iter().map(|l| self.intern(l.filename)).collect(),
                line_start_lines: loc.lines.iter().map(|l| l.start_line).collect(),
                line_numbers: loc.lines.iter().map(|l| l.line).collect(),
            };
            self.out.locations.push(row);
        }
        self.out.stacks.push(LandingStack {
            stack_id,
            node_ids,
            names,
            location_ids: ids,
        });
        stack_id
    }
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

    /// `id128` restates `compose128` as a bare integer, because
    /// `Fingerprint` has no accessor: this pins the two together.
    #[test]
    fn id128_is_compose128() {
        for buf in [&b""[..], b"abc", &[0xff; 100]] {
            assert_eq!(
                pulsus_model::Fingerprint::from_raw(id128(buf)),
                pulsus_model::compose128(buf)
            );
        }
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
