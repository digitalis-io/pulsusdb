//! OTLP profiles, development version 1: the request decoded from protobuf
//! or OTLP/JSON, and each profile converted to the pprof input the profile
//! model takes ([`crate::protocols::profile_model::parse_profiles`]), as the
//! reference converts OTLP to pprof and then takes its pprof path. Pure
//! functions, no I/O and no clock.
//!
//! One `ProfileInput` per resource, scope, profile and service group, in
//! that order; a profile's samples are grouped by their own `service.name`
//! attribute in first-seen order, so one request always converts to the same
//! inputs. Every check finishes before any input is returned: the first
//! refusal refuses the whole request.

use std::borrow::Cow;
use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::profiles::v1development as otlp;
use prost::Message;
use pulsus_profiles_wire::google_v1 as pprof;

use crate::error::LogsIngestError;
use crate::ingest::profiles::{ProfileInput, ProfileRefusal};
use crate::protocols::otlp_prescan::PROFILES_HEADER_LESS_NOTE;

const SERVICE_NAME: &str = "service.name";
const EXECUTABLE_NAME: &str = "process.executable.name";
const SDK_LANGUAGE: &str = "telemetry.sdk.language";
const BUILD_ID: &str = "process.executable.build_id.gnu";
const SERVICE_FALLBACK: &str = "unknown_service";
/// A profile without a duration is taken as ten seconds long.
const DEFAULT_DURATION_NS: i64 = 10_000_000_000;
/// H4: a header-less sample that an earlier version could have written with
/// its values where version 1 puts attributes.
const H4: &str = "a sample with attributes and timestamps but no values is refused without \
                  OTLP-Profiles-Development-Version, because earlier versions encode values in \
                  that field";

/// Decodes a protobuf body: the wire pre-scan (with the version check when
/// `header_less`), then `prost`.
pub fn decode(
    body: &[u8],
    header_less: bool,
) -> Result<ExportProfilesServiceRequest, LogsIngestError> {
    crate::protocols::otlp_prescan::prescan_profiles(body, header_less)?;
    Ok(ExportProfilesServiceRequest::decode(body)?)
}

/// Decodes an OTLP/JSON body through the bounded decoder.
pub fn decode_json(
    body: &[u8],
    header_less: bool,
) -> Result<ExportProfilesServiceRequest, LogsIngestError> {
    crate::protocols::otlp_json::profiles::decode_profiles(body, header_less)
}

fn refuse(message: impl Into<String>) -> ProfileRefusal {
    ProfileRefusal {
        message: message.into(),
    }
}

/// Converts a decoded request to the profile model's inputs. An empty
/// result is a request with nothing to store.
pub fn to_inputs(
    request: &ExportProfilesServiceRequest,
    header_less: bool,
) -> Result<Vec<ProfileInput>, ProfileRefusal> {
    let Some(dict) = request.dictionary.as_ref() else {
        return Err(refuse("missing profile metadata dictionary"));
    };
    if request.resource_profiles.is_empty() {
        return Err(refuse("missing resource profiles"));
    }
    let note = if header_less {
        PROFILES_HEADER_LESS_NOTE
    } else {
        ""
    };
    let mut out = Vec::new();
    for rp in &request.resource_profiles {
        let resource: &[KeyValue] = rp.resource.as_ref().map_or(&[], |r| &r.attributes);
        let resource_service = resource_service(resource, dict);
        let language = resource
            .iter()
            .find(|a| attribute_key(a, dict) == SDK_LANGUAGE)
            .map(|a| any_string(a.value.as_ref(), dict).into_owned())
            .filter(|l| !l.is_empty());
        for sp in &rp.scope_profiles {
            let scope: &[KeyValue] = sp.scope.as_ref().map_or(&[], |s| &s.attributes);
            for profile in &sp.profiles {
                let converted = convert(profile, dict, header_less).map_err(|reason| {
                    refuse(format!("failed to convert otel profile: {reason}{note}"))
                })?;
                let decompressed_bytes = profile.encoded_len() as u64;
                let id = profile_id(&profile.profile_id);
                for group in converted.groups {
                    let mut labels: Vec<(String, String)> = vec![
                        ("__delta__".to_owned(), "false".to_owned()),
                        ("__otel__".to_owned(), "true".to_owned()),
                        ("__name__".to_owned(), converted.name.to_owned()),
                    ];
                    append_unique(&mut labels, resource, dict);
                    append_unique(&mut labels, scope, dict);
                    let service = if group.service.is_empty() {
                        resource_service.clone()
                    } else {
                        group.service
                    };
                    labels.push(("service_name".to_owned(), service));
                    out.push(ProfileInput {
                        labels,
                        profile: group.profile,
                        decompressed_bytes,
                        id,
                        annotations: Vec::new(),
                        language: language.clone(),
                    });
                }
            }
        }
    }
    Ok(out)
}

/// `profile_id` when it is 16 bytes and not all zero.
fn profile_id(bytes: &[u8]) -> Option<[u8; 16]> {
    <[u8; 16]>::try_from(bytes)
        .ok()
        .filter(|id| id.iter().any(|b| *b != 0))
}

fn at<T>(table: &[T], i: i32) -> Option<&T> {
    usize::try_from(i).ok().and_then(|i| table.get(i))
}

/// A string-table entry, `""` when outside the table.
fn string_or_empty(dict: &otlp::ProfilesDictionary, i: i32) -> &str {
    at(&dict.string_table, i).map_or("", String::as_str)
}

/// An attribute's key: `key` when it is set or names no string-table entry,
/// else the entry `key_strindex` names.
fn attribute_key<'d>(attr: &'d KeyValue, dict: &'d otlp::ProfilesDictionary) -> &'d str {
    if !attr.key.is_empty() || attr.key_strindex == 0 {
        &attr.key
    } else {
        string_or_empty(dict, attr.key_strindex)
    }
}

/// A string, a string-table string or an integer, else `""`.
fn single_string<'d>(value: &'d AnyValue, dict: &'d otlp::ProfilesDictionary) -> Cow<'d, str> {
    match &value.value {
        Some(Value::StringValue(s)) => Cow::Borrowed(s),
        Some(Value::StringValueStrindex(i)) => Cow::Borrowed(string_or_empty(dict, *i)),
        Some(Value::IntValue(i)) => Cow::Owned(i.to_string()),
        _ => Cow::Borrowed(""),
    }
}

/// An attribute value as a label value: [`single_string`], or that of the
/// one element of an array, else `""`.
fn any_string<'d>(value: Option<&'d AnyValue>, dict: &'d otlp::ProfilesDictionary) -> Cow<'d, str> {
    let Some(value) = value else {
        return Cow::Borrowed("");
    };
    let single = single_string(value, dict);
    if !single.is_empty() {
        return single;
    }
    match &value.value {
        Some(Value::ArrayValue(array)) if array.values.len() == 1 => {
            single_string(&array.values[0], dict)
        }
        _ => Cow::Borrowed(""),
    }
}

/// The first non-empty `service.name`, else `unknown_service` with `:<name>`
/// for each non-empty `process.executable.name` before it.
fn resource_service(attrs: &[KeyValue], dict: &otlp::ProfilesDictionary) -> String {
    let mut fallback = SERVICE_FALLBACK.to_owned();
    for attr in attrs {
        let key = attribute_key(attr, dict);
        if key == SERVICE_NAME {
            let v = any_string(attr.value.as_ref(), dict);
            if !v.is_empty() {
                return v.into_owned();
            }
        }
        if key == EXECUTABLE_NAME {
            let v = any_string(attr.value.as_ref(), dict);
            if !v.is_empty() {
                fallback.push(':');
                fallback.push_str(&v);
            }
        }
    }
    fallback
}

/// Appends each attribute as a label, skipping an empty key, a key already
/// present and an empty value.
fn append_unique(
    labels: &mut Vec<(String, String)>,
    attrs: &[KeyValue],
    dict: &otlp::ProfilesDictionary,
) {
    for attr in attrs {
        let key = attribute_key(attr, dict);
        if key.is_empty() || labels.iter().any(|(k, _)| k == key) {
            continue;
        }
        let value = any_string(attr.value.as_ref(), dict);
        if !value.is_empty() {
            labels.push((key.to_owned(), value.into_owned()));
        }
    }
}

/// A dictionary attribute's value, `None` when the index is outside the
/// table.
fn dictionary_attribute(dict: &otlp::ProfilesDictionary, i: i32) -> Option<&otlp::KeyValueAndUnit> {
    at(&dict.attribute_table, i)
}

/// The value of the first of `indices` whose key is `key`, `""` when none
/// is.
fn attribute_value<'d>(
    dict: &'d otlp::ProfilesDictionary,
    indices: &[i32],
    key: &str,
) -> Cow<'d, str> {
    indices
        .iter()
        .filter_map(|&i| dictionary_attribute(dict, i))
        .find(|a| string_or_empty(dict, a.key_strindex) == key)
        .map_or(Cow::Borrowed(""), |a| any_string(a.value.as_ref(), dict))
}

/// One profile's conversion: its `__name__` and one pprof profile per
/// service group, in first-seen order.
struct Converted<'d> {
    name: &'d str,
    groups: Vec<Group>,
}

struct Group {
    /// The samples' `service.name`, `""` when they carry none.
    service: String,
    profile: pprof::Profile,
}

/// What every group of one profile shares.
struct Shape<'d> {
    sample_type: (&'d str, &'d str),
    period_type: Option<(&'d str, &'d str)>,
    period: i64,
    time_nanos: i64,
    duration_nanos: i64,
}

fn convert<'d>(
    p: &otlp::Profile,
    dict: &'d otlp::ProfilesDictionary,
    header_less: bool,
) -> Result<Converted<'d>, String> {
    if p.samples.is_empty() {
        return Ok(Converted {
            name: "",
            groups: Vec::new(),
        });
    }
    let string = |i: i32, what: &str| -> Result<&'d str, String> {
        at(&dict.string_table, i)
            .map(String::as_str)
            .ok_or_else(|| format!("could not access {what} string: index {i} out of bounds"))
    };
    let Some(sample_type) = p.sample_type else {
        return Err("sample type is missing".to_owned());
    };
    let sample_type = (
        string(sample_type.type_strindex, "type")?,
        string(sample_type.unit_strindex, "unit")?,
    );
    let period_type = match p.period_type {
        Some(vt) => Some((
            string(vt.type_strindex, "type")?,
            string(vt.unit_strindex, "unit")?,
        )),
        None => None,
    };
    let cpu = sample_type == ("samples", "count")
        && p.period != 0
        && period_type == Some(("cpu", "nanoseconds"));
    let off_cpu = !cpu
        && sample_type == ("off_cpu", "nanoseconds")
        && (p.period == 0 || period_type.is_none());
    let name = if cpu {
        "process_cpu"
    } else if off_cpu {
        "off_cpu"
    } else {
        match period_type {
            Some((t, _)) if p.period != 0 => t,
            _ => "",
        }
    };
    let shape = Shape {
        sample_type: if cpu {
            ("cpu", "nanoseconds")
        } else {
            sample_type
        },
        period_type,
        period: p.period,
        time_nanos: i64::try_from(p.time_unix_nano).unwrap_or(i64::MAX),
        duration_nanos: match p.duration_nano {
            0 => DEFAULT_DURATION_NS,
            d => i64::try_from(d).unwrap_or(i64::MAX),
        },
    };

    let mut groups: Vec<(String, Builder<'d>)> = Vec::new();
    let mut group_of: HashMap<String, usize> = HashMap::new();
    for sample in &p.samples {
        let value = check_sample(sample, dict, header_less)?;
        let value = if cpu {
            value.saturating_mul(p.period)
        } else {
            value
        };
        let service = attribute_value(dict, &sample.attribute_indices, SERVICE_NAME);
        let g = match group_of.get(service.as_ref()) {
            Some(&g) => g,
            None => {
                group_of.insert(service.clone().into_owned(), groups.len());
                groups.push((service.into_owned(), Builder::new(dict, &shape)));
                groups.len() - 1
            }
        };
        groups[g].1.add_sample(sample, value)?;
    }
    Ok(Converted {
        name,
        groups: groups
            .into_iter()
            .map(|(service, b)| Group {
                service,
                profile: b.profile,
            })
            .collect(),
    })
}

/// Steps 1 to 5 of one sample: the checks, in order, then its value.
fn check_sample(
    s: &otlp::Sample,
    dict: &otlp::ProfilesDictionary,
    header_less: bool,
) -> Result<i64, String> {
    let (values, timestamps) = (&s.values, &s.timestamps_unix_nano);
    if !values.is_empty() && !timestamps.is_empty() && values.len() != timestamps.len() {
        return Err(format!(
            "sample values length mismatch {} {}",
            values.len(),
            timestamps.len()
        ));
    }
    if values.is_empty() && timestamps.is_empty() {
        return Err("sample value is required".to_owned());
    }
    for (pos, &i) in s.attribute_indices.iter().enumerate() {
        if dictionary_attribute(dict, i).is_none() {
            return Err(format!(
                "could not access attribute at index {pos}: index {i} out of bounds"
            ));
        }
    }
    if s.link_index != 0 && at(&dict.link_table, s.link_index).is_none() {
        let i = s.link_index;
        return Err(format!(
            "could not access link at index {i}: index {i} out of bounds"
        ));
    }
    let Some(stack) = at(&dict.stack_table, s.stack_index) else {
        return Err(format!("invalid stack index: {}", s.stack_index));
    };
    for &li in &stack.location_indices {
        let Some(location) = at(&dict.location_table, li) else {
            return Err(format!(
                "could not access location at index {li}: index {li} out of bounds"
            ));
        };
        let mi = location.mapping_index;
        if at(&dict.mapping_table, mi).is_none() {
            return Err(format!(
                "could not access mapping at index {mi}: index {mi} out of bounds"
            ));
        }
        for line in &location.lines {
            let fi = line.function_index;
            if at(&dict.function_table, fi).is_none() {
                return Err(format!(
                    "could not access function: index {fi} out of bounds"
                ));
            }
        }
    }
    if header_less && !s.attribute_indices.is_empty() && !timestamps.is_empty() && values.is_empty()
    {
        return Err(H4.to_owned());
    }
    Ok(if values.is_empty() {
        i64::try_from(timestamps.len()).unwrap_or(i64::MAX)
    } else {
        values.iter().fold(0i64, |sum, v| sum.saturating_add(*v))
    })
}

/// One service group's pprof profile, its strings interned as they are
/// first used and its functions, mappings and locations numbered from 1 in
/// the order they are first reached.
struct Builder<'d> {
    dict: &'d otlp::ProfilesDictionary,
    strings: HashMap<Cow<'d, str>, i64>,
    functions: HashMap<i32, u64>,
    mappings: HashMap<i32, u64>,
    locations: HashMap<i32, u64>,
    profile: pprof::Profile,
}

impl<'d> Builder<'d> {
    fn new(dict: &'d otlp::ProfilesDictionary, shape: &Shape<'d>) -> Self {
        let mut b = Builder {
            dict,
            strings: HashMap::new(),
            functions: HashMap::new(),
            mappings: HashMap::new(),
            locations: HashMap::new(),
            profile: pprof::Profile {
                period: shape.period,
                time_nanos: shape.time_nanos,
                duration_nanos: shape.duration_nanos,
                ..pprof::Profile::default()
            },
        };
        b.intern(Cow::Borrowed(""));
        let sample_type = pprof::ValueType {
            r#type: b.intern(Cow::Borrowed(shape.sample_type.0)),
            unit: b.intern(Cow::Borrowed(shape.sample_type.1)),
        };
        b.profile.sample_type = vec![sample_type];
        b.profile.period_type = shape.period_type.map(|(t, u)| pprof::ValueType {
            r#type: b.intern(Cow::Borrowed(t)),
            unit: b.intern(Cow::Borrowed(u)),
        });
        b
    }

    fn intern(&mut self, s: Cow<'d, str>) -> i64 {
        if let Some(&i) = self.strings.get(s.as_ref()) {
            return i;
        }
        let i = self.profile.string_table.len() as i64;
        self.profile.string_table.push(s.as_ref().to_owned());
        self.strings.insert(s, i);
        i
    }

    /// A dictionary string, refused when outside the table.
    fn string(&mut self, i: i32, what: &str) -> Result<i64, String> {
        let dict = self.dict;
        let s = at(&dict.string_table, i)
            .ok_or_else(|| format!("could not access {what} string: index {i} out of bounds"))?;
        Ok(self.intern(Cow::Borrowed(s)))
    }

    /// Steps 6 and 8: the sample's labels and locations. Its indices were
    /// checked by [`check_sample`].
    fn add_sample(&mut self, s: &otlp::Sample, value: i64) -> Result<(), String> {
        let dict = self.dict;
        let mut label = Vec::new();
        for &ai in &s.attribute_indices {
            let Some(attr) = dictionary_attribute(dict, ai) else {
                continue;
            };
            let v = any_string(attr.value.as_ref(), dict);
            if v.is_empty() {
                continue;
            }
            let key = at(&dict.string_table, attr.key_strindex).ok_or_else(|| {
                format!(
                    "could not access attribute key string: index {} out of bounds",
                    attr.key_strindex
                )
            })?;
            if key == SERVICE_NAME {
                continue;
            }
            label.push(pprof::Label {
                key: self.intern(Cow::Borrowed(key)),
                str: self.intern(v),
                ..pprof::Label::default()
            });
        }
        if s.link_index != 0
            && let Some(link) = at(&dict.link_table, s.link_index)
        {
            label.push(pprof::Label {
                key: self.intern(Cow::Borrowed("span_id")),
                str: self.intern(Cow::Owned(hex(&link.span_id))),
                ..pprof::Label::default()
            });
            if link.trace_id.len() == 16 {
                label.push(pprof::Label {
                    key: self.intern(Cow::Borrowed("trace_id")),
                    str: self.intern(Cow::Owned(hex(&link.trace_id))),
                    ..pprof::Label::default()
                });
            }
        }
        let mut location_id = Vec::new();
        if let Some(stack) = at(&dict.stack_table, s.stack_index) {
            for &li in &stack.location_indices {
                location_id.push(self.location(li)?);
            }
        }
        self.profile.sample.push(pprof::Sample {
            location_id,
            value: vec![value],
            label,
        });
        Ok(())
    }

    fn location(&mut self, li: i32) -> Result<u64, String> {
        if let Some(&id) = self.locations.get(&li) {
            return Ok(id);
        }
        let dict = self.dict;
        let location = at(&dict.location_table, li).ok_or_else(|| {
            format!("could not access location at index {li}: index {li} out of bounds")
        })?;
        let mapping_id = self.mapping(location.mapping_index)?;
        let mut line = Vec::with_capacity(location.lines.len());
        for l in &location.lines {
            line.push(pprof::Line {
                function_id: self.function(l.function_index)?,
                line: l.line,
            });
        }
        let id = self.profile.location.len() as u64 + 1;
        self.profile.location.push(pprof::Location {
            id,
            mapping_id,
            address: location.address,
            line,
            is_folded: false,
        });
        self.locations.insert(li, id);
        Ok(id)
    }

    fn mapping(&mut self, mi: i32) -> Result<u64, String> {
        if let Some(&id) = self.mappings.get(&mi) {
            return Ok(id);
        }
        let dict = self.dict;
        let m = at(&dict.mapping_table, mi).ok_or_else(|| {
            format!("could not access mapping at index {mi}: index {mi} out of bounds")
        })?;
        let filename = self.string(m.filename_strindex, "mapping file name")?;
        let build_id = attribute_value(dict, &m.attribute_indices, BUILD_ID);
        let build_id = self.intern(build_id);
        let id = self.profile.mapping.len() as u64 + 1;
        self.profile.mapping.push(pprof::Mapping {
            id,
            memory_start: m.memory_start,
            memory_limit: m.memory_limit,
            file_offset: m.file_offset,
            filename,
            build_id,
            ..pprof::Mapping::default()
        });
        self.mappings.insert(mi, id);
        Ok(id)
    }

    fn function(&mut self, fi: i32) -> Result<u64, String> {
        if let Some(&id) = self.functions.get(&fi) {
            return Ok(id);
        }
        let dict = self.dict;
        let f = at(&dict.function_table, fi)
            .ok_or_else(|| format!("could not access function: index {fi} out of bounds"))?;
        let name = self.string(f.name_strindex, "function name")?;
        let system_name = self.string(f.system_name_strindex, "function system name")?;
        let filename = self.string(f.filename_strindex, "function file name")?;
        let id = self.profile.function.len() as u64 + 1;
        self.profile.function.push(pprof::Function {
            id,
            name,
            system_name,
            filename,
            start_line: f.start_line,
        });
        self.functions.insert(fi, id);
        Ok(id)
    }
}

/// Lower-case hex.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    out
}

#[cfg(test)]
mod tests;
