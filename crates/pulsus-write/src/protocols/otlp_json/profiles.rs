//! Bounded proto3-JSON for OTLP profiles, development version 1: the shape
//! of [`super::decode_traces`], at the protobuf pre-scan's caps
//! ([`crate::protocols::otlp_prescan`]). Containers take both spellings;
//! scalars are buffered and finished through the vendored derive
//! ([`super::finish_via_derive`]); scalar arrays are bounded by
//! [`super::metrics::accumulate_bounded_scalar_array`].
//!
//! **A request without `OTLP-Profiles-Development-Version` (H5).** In the
//! ten profiles seeds a key development version 1 does not define refuses
//! the request, so a body written by an earlier version is not read as
//! something else. With the header such a key is skipped, as every other
//! signal skips one. The shared `Resource` and `InstrumentationScope` seeds
//! are unchanged.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, Visitor};

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Link, Location, Mapping, Profile, ProfilesDictionary,
    ResourceProfiles, Sample, ScopeProfiles, Stack,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use crate::error::LogsIngestError;
use crate::protocols::otlp_prescan::{
    MAX_PROFILES, MAX_RESOURCE_PROFILES, MAX_SAMPLES, MAX_SCOPE_PROFILES,
    MAX_TOTAL_PROFILE_ENTRIES, MAX_TOTAL_SAMPLES, PROFILES_HEADER_LESS_NOTE,
};

use super::metrics::accumulate_bounded_scalar_array;
use super::{
    AggCharge, AnyValueSeed, InstrumentationScopeSeed, JsonAggregates, OptionSeed, ResourceSeed,
    accumulate_msgs, buffer_scalar_or_skip, finish_via_derive,
};

const RESOURCE_PROFILES_SCALARS: &[&str] = &["schemaUrl"];
const SCOPE_PROFILES_SCALARS: &[&str] = &["schemaUrl"];
const PROFILE_SCALARS: &[&str] = &[
    "sampleType",
    "timeUnixNano",
    "durationNano",
    "periodType",
    "period",
    "profileId",
    "droppedAttributesCount",
    "originalPayloadFormat",
    "originalPayload",
];
const SAMPLE_SCALARS: &[&str] = &["stackIndex", "linkIndex"];
const MAPPING_SCALARS: &[&str] = &[
    "memoryStart",
    "memoryLimit",
    "fileOffset",
    "filenameStrindex",
];
const LOCATION_SCALARS: &[&str] = &["mappingIndex", "address"];
const STACK_SCALARS: &[&str] = &[];
const KEY_VALUE_AND_UNIT_SCALARS: &[&str] = &["keyStrindex", "unitStrindex"];

type Pairs = Vec<(String, serde_json::Value)>;

/// What every profiles seed carries: the request's aggregates, whether the
/// request came without the version header, and the H5 refusal once one is
/// raised, so the caller can tell it from malformed JSON.
#[derive(Clone, Copy)]
struct Ctx<'a> {
    agg: &'a JsonAggregates,
    header_less: bool,
    refusal: &'a RefCell<Option<String>>,
}

impl<'a> Ctx<'a> {
    /// H5 for `key` in `message`, recorded for [`decode_profiles`].
    fn refuse<E: de::Error>(&self, message: &str, key: &str) -> E {
        let text = format!(
            "OTLP profiles: {message} has no field \"{key}\" in development version 1.{PROFILES_HEADER_LESS_NOTE}"
        );
        let mut slot = self.refusal.borrow_mut();
        if slot.is_none() {
            *slot = Some(text.clone());
        }
        E::custom(text)
    }

    /// A key no bounded field of `message` takes: a known scalar is
    /// buffered; anything else is H5 without the header, skipped with it.
    fn scalar<'de, A: MapAccess<'de>>(
        &self,
        message: &str,
        key: String,
        known: &[&str],
        map: &mut A,
        pairs: &mut Pairs,
    ) -> Result<(), A::Error> {
        if self.header_less && !known.contains(&key.as_str()) {
            return Err(self.refuse(message, &key));
        }
        buffer_scalar_or_skip(key, known, map, pairs)
    }

    /// A key of a message with no scalars of its own.
    fn skip<'de, A: MapAccess<'de>>(
        &self,
        message: &str,
        key: &str,
        map: &mut A,
    ) -> Result<(), A::Error> {
        if self.header_less {
            return Err(self.refuse(message, key));
        }
        map.next_value::<IgnoredAny>()?;
        Ok(())
    }

    fn samples(&self) -> AggCharge<'a> {
        AggCharge {
            cell: &self.agg.samples,
            cap: MAX_TOTAL_SAMPLES,
            field: "total samples",
        }
    }

    fn entries(&self) -> AggCharge<'a> {
        AggCharge {
            cell: &self.agg.profile_entries,
            cap: MAX_TOTAL_PROFILE_ENTRIES,
            field: "total dictionary entries",
        }
    }

    /// One dictionary table (or a location's lines) into `target`.
    fn table<'de, A, T, Mk, S>(
        &self,
        map: &mut A,
        target: &mut Vec<T>,
        field: &'static str,
        make_seed: Mk,
    ) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
        Mk: FnMut() -> S,
        S: DeserializeSeed<'de, Value = T>,
    {
        accumulate_msgs(
            map,
            target,
            (MAX_TOTAL_PROFILE_ENTRIES, field),
            Some(self.entries()),
            self.agg.byte_budget(),
            make_seed,
        )
    }

    /// A scalar array, appended to `target` for the derive.
    fn array<'de, A: MapAccess<'de>>(
        &self,
        map: &mut A,
        field: &'static str,
        target: &mut Option<Vec<serde_json::Value>>,
    ) -> Result<(), A::Error> {
        let value =
            accumulate_bounded_scalar_array(map, (MAX_SAMPLES, field), self.agg.byte_budget())?;
        if let serde_json::Value::Array(items) = value {
            target.get_or_insert_with(Vec::new).extend(items);
        }
        Ok(())
    }
}

/// Appends each scalar array that was given to `pairs` under its camelCase
/// name.
fn push_arrays<const N: usize>(
    pairs: &mut Pairs,
    arrays: [(&str, Option<Vec<serde_json::Value>>); N],
) {
    for (name, items) in arrays {
        if let Some(items) = items {
            pairs.push((name.to_owned(), serde_json::Value::Array(items)));
        }
    }
}

/// Decodes an OTLP/JSON `ExportProfilesServiceRequest` with every repeated
/// field bounded during deserialization. Malformed JSON, or a cap passed, is
/// [`LogsIngestError::DecodeJson`]; H5 is
/// [`LogsIngestError::ProfilesRequest`].
pub(crate) fn decode_profiles(
    body: &[u8],
    header_less: bool,
) -> Result<ExportProfilesServiceRequest, LogsIngestError> {
    let agg = JsonAggregates {
        string_value_strindex: true,
        ..JsonAggregates::default()
    };
    let refusal = RefCell::new(None);
    let ctx = Ctx {
        agg: &agg,
        header_less,
        refusal: &refusal,
    };
    let mut de = serde_json::Deserializer::from_slice(body);
    let decoded = RequestSeed { ctx }
        .deserialize(&mut de)
        .and_then(|req| de.end().map(|()| req));
    decoded.map_err(|e| match refusal.into_inner() {
        Some(message) => LogsIngestError::ProfilesRequest(message),
        None => LogsIngestError::DecodeJson(e),
    })
}

/// Implements `DeserializeSeed` as `deserialize_map(self)` and the
/// `expecting` text for a seed that is its own map visitor.
macro_rules! map_seed {
    ($seed:ident, $value:ty, $what:literal) => {
        impl<'de> DeserializeSeed<'de> for $seed<'_> {
            type Value = $value;

            fn deserialize<D>(self, deserializer: D) -> Result<$value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_map(self)
            }
        }

        impl $seed<'_> {
            fn expecting_text(f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str($what)
            }
        }
    };
}

struct RequestSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(
    RequestSeed,
    ExportProfilesServiceRequest,
    "a proto3-JSON ExportProfilesServiceRequest object"
);

impl<'de> Visitor<'de> for RequestSeed<'_> {
    type Value = ExportProfilesServiceRequest;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut resource_profiles: Vec<ResourceProfiles> = Vec::new();
        let mut dictionary: Option<ProfilesDictionary> = None;
        let mut dictionary_seen = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resourceProfiles" | "resource_profiles" => accumulate_msgs(
                    &mut map,
                    &mut resource_profiles,
                    (MAX_RESOURCE_PROFILES, "resourceProfiles"),
                    None,
                    ctx.agg.byte_budget(),
                    || ResourceProfilesSeed { ctx },
                )?,
                "dictionary" => {
                    if dictionary_seen {
                        return Err(de::Error::duplicate_field("dictionary"));
                    }
                    dictionary_seen = true;
                    dictionary = map.next_value_seed(OptionSeed(DictionarySeed { ctx }))?;
                }
                _ => ctx.skip("ExportProfilesServiceRequest", &key, &mut map)?,
            }
        }
        Ok(ExportProfilesServiceRequest {
            resource_profiles,
            dictionary,
        })
    }
}

struct ResourceProfilesSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(
    ResourceProfilesSeed,
    ResourceProfiles,
    "a proto3-JSON ResourceProfiles object"
);

impl<'de> Visitor<'de> for ResourceProfilesSeed<'_> {
    type Value = ResourceProfiles;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut resource: Option<Resource> = None;
        let mut resource_seen = false;
        let mut scope_profiles: Vec<ScopeProfiles> = Vec::new();
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resource" => {
                    if resource_seen {
                        return Err(de::Error::duplicate_field("resource"));
                    }
                    resource_seen = true;
                    resource = map.next_value_seed(OptionSeed(ResourceSeed { agg: ctx.agg }))?;
                }
                "scopeProfiles" | "scope_profiles" => accumulate_msgs(
                    &mut map,
                    &mut scope_profiles,
                    (MAX_SCOPE_PROFILES, "scopeProfiles"),
                    None,
                    ctx.agg.byte_budget(),
                    || ScopeProfilesSeed { ctx },
                )?,
                _ => ctx.scalar(
                    "ResourceProfiles",
                    key,
                    RESOURCE_PROFILES_SCALARS,
                    &mut map,
                    &mut pairs,
                )?,
            }
        }
        let mut out: ResourceProfiles = finish_via_derive(&pairs)?;
        out.resource = resource;
        out.scope_profiles = scope_profiles;
        Ok(out)
    }
}

struct ScopeProfilesSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(
    ScopeProfilesSeed,
    ScopeProfiles,
    "a proto3-JSON ScopeProfiles object"
);

impl<'de> Visitor<'de> for ScopeProfilesSeed<'_> {
    type Value = ScopeProfiles;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut scope: Option<InstrumentationScope> = None;
        let mut scope_seen = false;
        let mut profiles: Vec<Profile> = Vec::new();
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "scope" => {
                    if scope_seen {
                        return Err(de::Error::duplicate_field("scope"));
                    }
                    scope_seen = true;
                    scope =
                        map.next_value_seed(OptionSeed(InstrumentationScopeSeed { agg: ctx.agg }))?;
                }
                "profiles" => accumulate_msgs(
                    &mut map,
                    &mut profiles,
                    (MAX_PROFILES, "profiles"),
                    None,
                    ctx.agg.byte_budget(),
                    || ProfileSeed { ctx },
                )?,
                _ => ctx.scalar(
                    "ScopeProfiles",
                    key,
                    SCOPE_PROFILES_SCALARS,
                    &mut map,
                    &mut pairs,
                )?,
            }
        }
        let mut out: ScopeProfiles = finish_via_derive(&pairs)?;
        out.scope = scope;
        out.profiles = profiles;
        Ok(out)
    }
}

struct ProfileSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(ProfileSeed, Profile, "a proto3-JSON Profile object");

impl<'de> Visitor<'de> for ProfileSeed<'_> {
    type Value = Profile;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut samples: Vec<Sample> = Vec::new();
        let mut attribute_indices = None;
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "samples" => accumulate_msgs(
                    &mut map,
                    &mut samples,
                    (MAX_SAMPLES, "samples"),
                    Some(ctx.samples()),
                    ctx.agg.byte_budget(),
                    || SampleSeed { ctx },
                )?,
                "attributeIndices" | "attribute_indices" => {
                    ctx.array(&mut map, "attributeIndices", &mut attribute_indices)?
                }
                _ => ctx.scalar("Profile", key, PROFILE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        push_arrays(&mut pairs, [("attributeIndices", attribute_indices)]);
        let mut out: Profile = finish_via_derive(&pairs)?;
        out.samples = samples;
        Ok(out)
    }
}

struct SampleSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(SampleSeed, Sample, "a proto3-JSON Sample object");

impl<'de> Visitor<'de> for SampleSeed<'_> {
    type Value = Sample;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let (mut attribute_indices, mut values, mut timestamps) = (None, None, None);
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributeIndices" | "attribute_indices" => {
                    ctx.array(&mut map, "attributeIndices", &mut attribute_indices)?
                }
                "values" => ctx.array(&mut map, "values", &mut values)?,
                "timestampsUnixNano" | "timestamps_unix_nano" => {
                    ctx.array(&mut map, "timestampsUnixNano", &mut timestamps)?
                }
                _ => ctx.scalar("Sample", key, SAMPLE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        push_arrays(
            &mut pairs,
            [
                ("attributeIndices", attribute_indices),
                ("values", values),
                ("timestampsUnixNano", timestamps),
            ],
        );
        finish_via_derive(&pairs)
    }
}

struct DictionarySeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(
    DictionarySeed,
    ProfilesDictionary,
    "a proto3-JSON ProfilesDictionary object"
);

impl<'de> Visitor<'de> for DictionarySeed<'_> {
    type Value = ProfilesDictionary;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut d = ProfilesDictionary::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "mappingTable" | "mapping_table" => {
                    ctx.table(&mut map, &mut d.mapping_table, "mappingTable", || {
                        MappingSeed { ctx }
                    })?
                }
                "locationTable" | "location_table" => {
                    ctx.table(&mut map, &mut d.location_table, "locationTable", || {
                        LocationSeed { ctx }
                    })?
                }
                "functionTable" | "function_table" => {
                    ctx.table(&mut map, &mut d.function_table, "functionTable", || {
                        PhantomData::<Function>
                    })?
                }
                "linkTable" | "link_table" => {
                    ctx.table(&mut map, &mut d.link_table, "linkTable", || {
                        PhantomData::<Link>
                    })?
                }
                "stringTable" | "string_table" => {
                    ctx.table(&mut map, &mut d.string_table, "stringTable", || {
                        PhantomData::<String>
                    })?
                }
                "attributeTable" | "attribute_table" => {
                    ctx.table(&mut map, &mut d.attribute_table, "attributeTable", || {
                        KeyValueAndUnitSeed { ctx }
                    })?
                }
                "stackTable" | "stack_table" => {
                    ctx.table(&mut map, &mut d.stack_table, "stackTable", || StackSeed {
                        ctx,
                    })?
                }
                _ => ctx.skip("ProfilesDictionary", &key, &mut map)?,
            }
        }
        Ok(d)
    }
}

struct MappingSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(MappingSeed, Mapping, "a proto3-JSON Mapping object");

impl<'de> Visitor<'de> for MappingSeed<'_> {
    type Value = Mapping;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut attribute_indices = None;
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributeIndices" | "attribute_indices" => {
                    ctx.array(&mut map, "attributeIndices", &mut attribute_indices)?
                }
                _ => ctx.scalar("Mapping", key, MAPPING_SCALARS, &mut map, &mut pairs)?,
            }
        }
        push_arrays(&mut pairs, [("attributeIndices", attribute_indices)]);
        finish_via_derive(&pairs)
    }
}

struct LocationSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(LocationSeed, Location, "a proto3-JSON Location object");

impl<'de> Visitor<'de> for LocationSeed<'_> {
    type Value = Location;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut lines: Vec<Line> = Vec::new();
        let mut attribute_indices = None;
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "lines" => ctx.table(&mut map, &mut lines, "lines", || PhantomData::<Line>)?,
                "attributeIndices" | "attribute_indices" => {
                    ctx.array(&mut map, "attributeIndices", &mut attribute_indices)?
                }
                _ => ctx.scalar("Location", key, LOCATION_SCALARS, &mut map, &mut pairs)?,
            }
        }
        push_arrays(&mut pairs, [("attributeIndices", attribute_indices)]);
        let mut out: Location = finish_via_derive(&pairs)?;
        out.lines = lines;
        Ok(out)
    }
}

struct StackSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(StackSeed, Stack, "a proto3-JSON Stack object");

impl<'de> Visitor<'de> for StackSeed<'_> {
    type Value = Stack;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut location_indices = None;
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "locationIndices" | "location_indices" => {
                    ctx.array(&mut map, "locationIndices", &mut location_indices)?
                }
                _ => ctx.scalar("Stack", key, STACK_SCALARS, &mut map, &mut pairs)?,
            }
        }
        push_arrays(&mut pairs, [("locationIndices", location_indices)]);
        finish_via_derive(&pairs)
    }
}

struct KeyValueAndUnitSeed<'a> {
    ctx: Ctx<'a>,
}
map_seed!(
    KeyValueAndUnitSeed,
    KeyValueAndUnit,
    "a proto3-JSON KeyValueAndUnit object"
);

impl<'de> Visitor<'de> for KeyValueAndUnitSeed<'_> {
    type Value = KeyValueAndUnit;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Self::expecting_text(f)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let ctx = self.ctx;
        let mut value: Option<AnyValue> = None;
        let mut value_seen = false;
        let mut pairs: Pairs = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            if key == "value" {
                if value_seen {
                    return Err(de::Error::duplicate_field("value"));
                }
                value_seen = true;
                value = map.next_value_seed(OptionSeed(AnyValueSeed {
                    agg: ctx.agg,
                    depth: 1,
                }))?;
            } else {
                ctx.scalar(
                    "KeyValueAndUnit",
                    key,
                    KEY_VALUE_AND_UNIT_SCALARS,
                    &mut map,
                    &mut pairs,
                )?;
            }
        }
        let mut out: KeyValueAndUnit = finish_via_derive(&pairs)?;
        out.value = value;
        Ok(out)
    }
}
