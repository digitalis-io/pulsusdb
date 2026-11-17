//! The two profile push transports' request rules, ahead of the pprof model
//! (`profile_model`): the push route's series and samples become
//! [`ProfileInput`]s ([`push_inputs`], `pkg/distributor/distributor.go:360-457`),
//! and `/ingest`'s query parameters, multipart form and pprof body become one
//! ([`parse_legacy_query`], [`parse_multipart`], [`legacy_pprof_input`],
//! `pkg/og/convert/pprof/profile.go:51-349`). Pure functions, no I/O; the
//! receive time is a parameter.

use std::collections::HashMap;

use prost::Message;
use pulsus_profiles_wire::connect::Code;
use pulsus_profiles_wire::google_v1::{Function, Profile};
use pulsus_profiles_wire::push_v1::PushRequest;

use crate::ingest::profiles::ProfileInput;
use crate::protocols::profile_model::{
    MAX_PROFILE_SIZE_BYTES, PprofDecodeError, SPY_LABEL, decode_pprof_sized,
};

/// A label set, in first-seen order.
pub type Labels = Vec<(String, String)>;

/// `/ingest`'s refusal of a form that carries `prev_profile`, verbatim from
/// `pkg/og/convert/pprof/profile.go:161-162`.
pub const PREV_PROFILE_MESSAGE: &str = "unsupported client version. \
     Please update github.com/grafana/pyroscope-go to the latest version";

/// The bound on the sum of a push's decompressed profiles
/// (`pkg/distributor/distributor.go:371-380`).
pub const MAX_BATCH_DECOMPRESSED_BYTES: u64 = 268_435_456;

/// `UTC 1971-01-01T00:00:00Z` in nanoseconds: a profile time below it is in
/// 1970, and `/ingest` replaces it (`profile.go:110-118`).
const NS_1971: i64 = 31_536_000_000_000_000;

const NS_PER_S: i64 = 1_000_000_000;

/// Why the push route refused a push before the model ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushInputError {
    pub code: Code,
    pub message: String,
}

fn invalid(message: &str) -> PushInputError {
    PushInputError {
        code: Code::InvalidArgument,
        message: message.to_string(),
    }
}

/// The push route's series and samples, in order, as the model's inputs
/// (§3.1 steps 6 to 8). Every profile is decoded before any is returned, so a
/// refusal of one refuses the push. Client annotations are dropped, as
/// `api/push/v1/push.proto:31-39` states and `distributor.go:381-387` does.
pub fn push_inputs(req: PushRequest) -> Result<Vec<ProfileInput>, PushInputError> {
    let mut inputs = Vec::new();
    let mut decompressed: u64 = 0;
    for series in req.series {
        let labels: Labels = series
            .labels
            .into_iter()
            .map(|l| (l.name, l.value))
            .collect();
        for sample in series.samples {
            let (profile, size) = decode_pprof_sized(&sample.raw_profile, MAX_PROFILE_SIZE_BYTES)
                .map_err(|e| match e {
                PprofDecodeError::TooLarge { .. } => {
                    invalid("uncompressed profile payload size exceeds limit of 4.2 MB")
                }
                PprofDecodeError::Other(message) => PushInputError {
                    code: Code::Unknown,
                    message,
                },
            })?;
            decompressed = decompressed.saturating_add(size);
            inputs.push(ProfileInput {
                labels: labels.clone(),
                decompressed_bytes: profile.encoded_len() as u64,
                profile,
                id: parse_uuid(&sample.id),
                annotations: Vec::new(),
                language: None,
            });
        }
    }
    if decompressed > MAX_BATCH_DECOMPRESSED_BYTES {
        return Err(invalid(
            "uncompressed batched profile payload size exceeds limit of 268 MB",
        ));
    }
    if inputs.is_empty() {
        return Err(invalid("no profiles received"));
    }
    Ok(inputs)
}

/// The 16 bytes of `s` when it is a UUID in a form Go's `uuid.Parse` takes:
/// 36 characters with hyphens, 32 hex digits, either inside `{}` or after
/// `urn:uuid:`, any case (`distributor.go:410-424`).
pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let s = if s.len() == 45
        && s.get(..9)
            .is_some_and(|p| p.eq_ignore_ascii_case("urn:uuid:"))
    {
        &s[9..]
    } else if s.len() == 38 && s.starts_with('{') && s.ends_with('}') {
        &s[1..37]
    } else {
        s
    };
    let hex: Vec<u8> = match s.len() {
        36 => {
            let b = s.as_bytes();
            if [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
                return None;
            }
            b.iter().copied().filter(|&c| c != b'-').collect()
        }
        32 => s.as_bytes().to_vec(),
        _ => return None,
    };
    if hex.len() != 32 {
        return None;
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        out[i] = digit(pair[0])? << 4 | digit(pair[1])?;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// `/ingest`: query parameters
// ---------------------------------------------------------------------------

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
    /// `sampleRate`: 100 when absent, unreadable or negative.
    pub sample_rate: u32,
}

/// Reads the query string as Go's `url.URL.Query().Get` does: the first
/// value of each key, `+` and `%XX` decoded; a pair that does not decode is
/// skipped.
fn query_pairs(raw: &str) -> HashMap<String, String> {
    fn unescape(s: &str) -> Option<String> {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            match b[i] {
                b'+' => out.push(b' '),
                b'%' => {
                    let hex = b.get(i + 1..i + 3)?;
                    let hex = std::str::from_utf8(hex).ok()?;
                    out.push(u8::from_str_radix(hex, 16).ok()?);
                    i += 2;
                }
                c => out.push(c),
            }
            i += 1;
        }
        String::from_utf8(out).ok()
    }
    let mut pairs = HashMap::new();
    for pair in raw.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if let (Some(k), Some(v)) = (unescape(k), unescape(v)) {
            pairs.entry(k).or_insert(v);
        }
    }
    pairs
}

/// Reads `/ingest`'s query string (`pkg/ingester/…/ingest_handler.go:120-185`).
/// A bad `name` is refused with `name: {error}`; a `sampleRate` that does
/// not read as an integer, or is negative, is the default 100.
pub fn parse_legacy_query(raw: Option<&str>, now_ns: i64) -> Result<LegacyQuery, String> {
    let q = query_pairs(raw.unwrap_or(""));
    let get = |k: &str| q.get(k).map(String::as_str).filter(|v| !v.is_empty());
    let name = parse_name(get("name").unwrap_or("")).map_err(|e| format!("name: {e}"))?;
    Ok(LegacyQuery {
        name,
        from_ns: get("from").map(|f| parse_at_time(f, now_ns)),
        spy_name: get("spyName").unwrap_or("unknown").to_string(),
        format: get("format").map(str::to_string),
        sample_rate: get("sampleRate")
            .and_then(|r| r.parse::<i64>().ok())
            .filter(|r| *r >= 0)
            .map_or(100, |r| r as u32),
    })
}

/// Go's `%q` of one character.
fn go_quote_rune(c: char) -> String {
    match c {
        '\'' => "'\\''".to_string(),
        '\\' => "'\\\\'".to_string(),
        c if c.is_control() => format!("'\\u{:04x}'", c as u32),
        c => format!("'{c}'"),
    }
}

fn label_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.'
}

fn validate_service_name(n: &str) -> Result<(), String> {
    if n.is_empty() {
        return Err("service name is required".to_string());
    }
    match n
        .chars()
        .find(|&c| !(label_name_char(c) || c == '-' || c == '/'))
    {
        Some(c) => Err(format!(
            "invalid service name: {n}: character is not allowed: {}",
            go_quote_rune(c)
        )),
        None => Ok(()),
    }
}

fn validate_label_name(k: &str) -> Result<(), String> {
    if k.is_empty() {
        return Err("label name is required".to_string());
    }
    match k.chars().find(|&c| !label_name_char(c)) {
        Some(c) => Err(format!(
            "invalid label name: {k}: character is not allowed: {}",
            go_quote_rune(c)
        )),
        None => Ok(()),
    }
}

/// `labelset.Parse` (`api/model/labelset/labelset.go:30-149`): the text is
/// read with `{` appended, one character at a time. The service name is the
/// `__name__` label; a later key replaces an earlier one.
pub fn parse_name(name: &str) -> Result<Labels, String> {
    enum State {
        Name,
        Key,
        Value,
        Done,
    }
    fn set(ls: &mut Labels, k: String, v: String) {
        match ls.iter_mut().find(|(n, _)| *n == k) {
            Some(slot) => slot.1 = v,
            None => ls.push((k, v)),
        }
    }
    let mut labels = Labels::new();
    let mut state = State::Name;
    let (mut key, mut value) = (String::new(), String::new());
    for c in name.chars().chain(std::iter::once('{')) {
        match state {
            State::Name => match c {
                '{' => {
                    let service = value.trim().to_string();
                    validate_service_name(&service)?;
                    set(&mut labels, "__name__".to_string(), service);
                    state = State::Key;
                }
                c => value.push(c),
            },
            State::Key => match c {
                '}' => state = State::Done,
                '=' => {
                    value.clear();
                    state = State::Value;
                }
                c => key.push(c),
            },
            State::Value => match c {
                ',' | '}' => {
                    let k = key.trim().to_string();
                    if k != "__name__" {
                        validate_label_name(&k)?;
                    }
                    set(&mut labels, k, value.trim().to_string());
                    key.clear();
                    state = State::Key;
                }
                c => value.push(c),
            },
            State::Done => {}
        }
    }
    Ok(labels)
}

/// `attime.Parse` (`pkg/og/util/attime/attime.go`), in nanoseconds: digits
/// are a date (`YYYYMMDD`) or a Unix time by length; anything else is now
/// plus the offset from its first `+`, or else its first `-`.
pub fn parse_at_time(s: &str, now_ns: i64) -> i64 {
    let s: String = s
        .trim()
        .chars()
        .filter(|c| !matches!(c, '_' | ',' | ' '))
        .collect();
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        if s.len() == 8
            && let Some(ns) = date_ns(&s)
        {
            return ns;
        }
        // Go's `strconv.Atoi` saturates out of range.
        let v: i64 = s.parse().unwrap_or(i64::MAX);
        return match s.len() {
            13 => v.saturating_mul(1_000_000),
            16 => v.saturating_mul(1_000),
            19 => v,
            _ => v.saturating_mul(NS_PER_S),
        };
    }
    let offset = match s.find('+').or_else(|| s.find('-')) {
        Some(i) => &s[i..],
        None => "",
    };
    now_ns.saturating_add(offset_ns(offset))
}

/// `YYYYMMDD` as that UTC midnight, when it is a date after 1900.
fn date_ns(s: &str) -> Option<i64> {
    let (y, m, d): (i64, u32, u32) = (
        s[..4].parse().ok()?,
        s[4..6].parse().ok()?,
        s[6..].parse().ok()?,
    );
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days_in_month = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if y <= 1900 || d == 0 || d > days_in_month {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 * NS_PER_S)
}

/// `parseTimeOffset`: a sign, the leading digits, and a unit that is the
/// rest of the text, read by its prefix.
fn offset_ns(offset: &str) -> i64 {
    let (sign, rest) = match offset.as_bytes().first() {
        Some(b'-') => (-1, &offset[1..]),
        Some(b'+') => (1, &offset[1..]),
        Some(_) => (1, offset),
        None => return 0,
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let num: i64 = rest[..digits].parse().unwrap_or(0);
    let unit = &rest[digits..];
    let seconds: i64 = if unit.starts_with('s') {
        1
    } else if unit.starts_with("mon") || unit.starts_with('M') {
        86_400 * 30
    } else if unit.starts_with("min") || unit.starts_with('m') {
        60
    } else if unit.starts_with('h') {
        3_600
    } else if unit.starts_with('d') {
        86_400
    } else if unit.starts_with('w') {
        86_400 * 7
    } else if unit.starts_with('y') {
        86_400 * 365
    } else {
        0
    };
    (sign * num)
        .saturating_mul(seconds)
        .saturating_mul(NS_PER_S)
}

// ---------------------------------------------------------------------------
// `/ingest`: the multipart form
// ---------------------------------------------------------------------------

/// The `sample_type_config` form part: every sample type it names, with its
/// display name (empty when it gives none).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SampleTypeConfig {
    display_names: HashMap<String, String>,
}

impl SampleTypeConfig {
    /// Reads the part's JSON object: type name to `{"display-name": …}`,
    /// other keys ignored (`profile.go:165-173`).
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct Entry {
            #[serde(rename = "display-name", default)]
            display_name: Option<String>,
        }
        let map: HashMap<String, Option<Entry>> =
            serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        Ok(SampleTypeConfig {
            display_names: map
                .into_iter()
                .map(|(k, v)| (k, v.and_then(|e| e.display_name).unwrap_or_default()))
                .collect(),
        })
    }

    /// The default display names (`pkg/og/storage/tree/pprof.go:40-81`).
    fn default_mapping() -> Self {
        let names = [
            ("samples", "cpu"),
            ("inuse_objects", ""),
            ("alloc_objects", ""),
            ("inuse_space", ""),
            ("alloc_space", ""),
            ("goroutine", "goroutines"),
            ("contentions", "mutex_count"),
            ("delay", "mutex_duration"),
        ];
        SampleTypeConfig {
            display_names: names
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn display_name(&self, sample_type: &str) -> Option<&str> {
        self.display_names
            .get(sample_type)
            .map(String::as_str)
            .filter(|d| !d.is_empty())
    }

    fn has(&self, sample_type: &str) -> bool {
        self.display_names.contains_key(sample_type)
    }
}

/// The multipart form's parts `/ingest` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Multipart {
    pub profile: Option<Vec<u8>>,
    pub sample_type_config: Option<SampleTypeConfig>,
}

/// The `boundary` parameter of a `Content-Type`, quoted or not.
fn boundary_of(content_type: &str) -> Option<String> {
    content_type.split(';').skip(1).find_map(|param| {
        let (k, v) = param.split_once('=')?;
        if !k.trim().eq_ignore_ascii_case("boundary") {
            return None;
        }
        let v = v.trim();
        let v = v
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(v);
        Some(v.to_string())
    })
}

/// A `Content-Disposition` parameter's value, quoted or not.
fn disposition_param(value: &str, name: &str) -> Option<String> {
    value.split(';').skip(1).find_map(|param| {
        let (k, v) = param.split_once('=')?;
        if !k.trim().eq_ignore_ascii_case(name) {
            return None;
        }
        let v = v.trim();
        Some(
            v.strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(v)
                .to_string(),
        )
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Every file part of the form: `(name, bytes)`, in order. A part counts as
/// a file only when its `Content-Disposition` has a `filename`.
fn file_parts<'a>(body: &'a [u8], boundary: &str) -> Result<Vec<(String, &'a [u8])>, String> {
    let open = format!("--{boundary}");
    let between = format!("\r\n--{boundary}");
    let start = if body.starts_with(open.as_bytes()) {
        0
    } else {
        find(body, between.as_bytes()).ok_or("multipart: NextPart: EOF")? + 2
    };
    let mut rest = &body[start + open.len()..];
    let mut parts = Vec::new();
    loop {
        if rest.starts_with(b"--") {
            return Ok(parts);
        }
        let after_line = find(rest, b"\r\n").ok_or("multipart: NextPart: EOF")? + 2;
        rest = &rest[after_line..];
        let head_end = find(rest, b"\r\n\r\n").ok_or("multipart: NextPart: EOF")?;
        let head = String::from_utf8_lossy(&rest[..head_end]).into_owned();
        rest = &rest[head_end + 4..];
        let end = find(rest, between.as_bytes()).ok_or("multipart: NextPart: EOF")?;
        let content = &rest[..end];
        rest = &rest[end + between.len()..];
        let disposition = head.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-disposition")
                .then(|| v.trim().to_string())
        });
        if let Some(d) = disposition
            && let Some(name) = disposition_param(&d, "name")
            && disposition_param(&d, "filename").is_some()
        {
            parts.push((name, content));
        }
    }
}

/// Reads `/ingest`'s multipart form (`profile.go:137-175`,
/// `pkg/og/util/form/form.go`): the first non-empty file part of each name.
pub fn parse_multipart(content_type: &str, body: &[u8]) -> Result<Multipart, String> {
    let boundary = boundary_of(content_type).ok_or("malformed multipart content type header")?;
    let parts = file_parts(body, &boundary)?;
    let field = |name: &str| {
        parts
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| *b)
            .filter(|b| !b.is_empty())
    };
    if field("prev_profile").is_some() {
        return Err(PREV_PROFILE_MESSAGE.to_string());
    }
    let sample_type_config = field("sample_type_config")
        .map(SampleTypeConfig::parse)
        .transpose()?;
    Ok(Multipart {
        profile: field("profile").map(<[u8]>::to_vec),
        sample_type_config,
    })
}

// ---------------------------------------------------------------------------
// `/ingest`: the pprof profile
// ---------------------------------------------------------------------------

fn st(table: &[String], i: i64) -> &str {
    usize::try_from(i)
        .ok()
        .and_then(|i| table.get(i))
        .map_or("", String::as_str)
}

/// The series name, from the sample types and their display names
/// (`profile.go:177-211`). `config` replaces the default whole.
pub fn series_name(profile: &Profile, config: Option<&SampleTypeConfig>) -> String {
    let table = &profile.string_table;
    if profile
        .sample_type
        .iter()
        .any(|vt| st(table, vt.r#type) == "wall")
    {
        return "wall".to_string();
    }
    let default = SampleTypeConfig::default_mapping();
    let config = config.unwrap_or(&default);
    let mut last = "";
    for vt in &profile.sample_type {
        let name = st(table, vt.r#type);
        let name = config.display_name(name).unwrap_or(name);
        last = name;
        if name.contains("cpu") {
            return "process_cpu".to_string();
        }
        if name.contains("alloc_")
            || name.contains("inuse_")
            || name == "space"
            || name == "objects"
        {
            return "memory".to_string();
        }
        if name.contains("mutex_") {
            return "mutex".to_string();
        }
        if name.contains("block_") {
            return "block".to_string();
        }
        if name.contains("goroutines") {
            return "goroutines".to_string();
        }
    }
    last.to_string()
}

/// Scripting spies: each line's function becomes `"{file} {name}"`, a new
/// function with a new string, except under `pyspy` when the file is an
/// absolute path (`profile.go:269-325`).
fn fix_scripting_names(p: &mut Profile, spy: &str) {
    let mut added: HashMap<String, i64> = HashMap::new();
    let index: HashMap<u64, usize> = p
        .function
        .iter()
        .enumerate()
        .map(|(i, f)| (f.id, i))
        .collect();
    let mut max_id = p.function.iter().map(|f| f.id).max().unwrap_or(0);
    let mut by_name: HashMap<String, u64> = HashMap::new();
    for li in 0..p.location.len() {
        for ni in 0..p.location[li].line.len() {
            let fid = p.location[li].line[ni].function_id;
            let Some(&fi) = index.get(&fid) else {
                continue;
            };
            let f = p.function[fi];
            let file = st(&p.string_table, f.filename);
            if spy == "pyspy" && file.starts_with('/') {
                continue;
            }
            let name = format!("{file} {}", st(&p.string_table, f.name));
            let id = match by_name.get(&name) {
                Some(&id) => id,
                None => {
                    let sid = match added.get(&name) {
                        Some(&sid) => sid,
                        None => {
                            p.string_table.push(name.clone());
                            let sid = (p.string_table.len() - 1) as i64;
                            added.insert(name.clone(), sid);
                            sid
                        }
                    };
                    max_id += 1;
                    p.function.push(Function {
                        id: max_id,
                        name: sid,
                        filename: f.filename,
                        system_name: f.system_name,
                        start_line: f.start_line,
                    });
                    by_name.insert(name, max_id);
                    max_id
                }
            };
            p.location[li].line[ni].function_id = id;
        }
    }
}

/// .NET: function ids all 0 and as many functions as locations take the
/// locations' ids; `-` in sample-type names becomes `_` (`profile.go:327-349`).
fn fix_dotnet(p: &mut Profile) {
    if p.function.iter().all(|f| f.id == 0) && p.function.len() == p.location.len() {
        for (f, l) in p.function.iter_mut().zip(&p.location) {
            f.id = l.id;
        }
    }
    for vt in p.sample_type.clone() {
        if let Some(s) = usize::try_from(vt.r#type)
            .ok()
            .and_then(|i| p.string_table.get_mut(i))
            && s.contains('-')
        {
            *s = s.replace('-', "_");
        }
    }
}

/// One `/ingest` pprof profile as the model's input (§4.4 steps 2 to 6, in
/// `profile.go:51-100`'s order): its time, the scripting and .NET fixes, and
/// the reference's labels.
pub fn legacy_pprof_input(
    mut profile: Profile,
    query: &LegacyQuery,
    config: Option<&SampleTypeConfig>,
) -> ProfileInput {
    if (0..NS_1971).contains(&profile.time_nanos) {
        profile.time_nanos = query.from_ns.unwrap_or(0);
    }
    let spy = query.spy_name.as_str();
    if matches!(spy, "pyspy" | "rbspy" | "scripting") {
        fix_scripting_names(&mut profile, spy);
    }
    let default = SampleTypeConfig::default_mapping();
    if spy == "dotnetspy" || (spy == "unknown" && config.unwrap_or(&default).has("inuse-space")) {
        fix_dotnet(&mut profile);
    }

    let service = query
        .name
        .iter()
        .find(|(k, _)| k == "__name__")
        .map_or("", |(_, v)| v.as_str());
    let mut labels: Labels = vec![
        ("__name__".to_string(), series_name(&profile, config)),
        ("__delta__".to_string(), "false".to_string()),
        (SPY_LABEL.to_string(), query.spy_name.clone()),
    ];
    if !query.name.iter().any(|(k, _)| k == "service_name") {
        labels.push(("service_name".to_string(), service.to_string()));
    }
    labels.extend(
        query
            .name
            .iter()
            .filter(|(k, _)| !k.starts_with("__"))
            .cloned(),
    );
    ProfileInput {
        labels,
        decompressed_bytes: profile.encoded_len() as u64,
        profile,
        id: None,
        annotations: Vec::new(),
        language: None,
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
            sample_rate: 100,
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

    /// A 45-byte id whose ninth byte falls inside a character is not a
    /// UUID, and is ignored rather than panicking.
    #[test]
    fn a_45_byte_id_split_inside_a_character_is_ignored() {
        let id = format!("urn:uuid\u{e9}{}", "0".repeat(35));
        assert_eq!(id.len(), 45);
        assert!(!id.is_char_boundary(9));
        assert_eq!(parse_uuid(&id), None);
        let urn = "URN:UUID:734fd599-6865-419e-9475-932762d8f469";
        assert_eq!(parse_uuid(urn).map(|b| b[0]), Some(0x73));
    }
}
