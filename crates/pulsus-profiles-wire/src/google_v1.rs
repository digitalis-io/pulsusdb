//! `google.v1`, the pprof profile message, with the field numbers and types
//! of `proto/google/v1/profile.proto`. String fields other than
//! `string_table` are indices into `string_table`. Issue #828: the proto3
//! JSON mapping as `serde` attributes — lowerCamelCase names, a 64-bit
//! number as a string, a field at its default omitted; read by its JSON or
//! its proto name.

use serde::{Deserialize, Serialize};

const fn is_false(v: &bool) -> bool {
    !*v
}

/// A profile: its sample types, samples, and the tables they index.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Profile {
    #[prost(message, repeated, tag = "1")]
    #[serde(
        rename = "sampleType",
        alias = "sample_type",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub sample_type: Vec<ValueType>,
    #[prost(message, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mapping: Vec<Mapping>,
    #[prost(message, repeated, tag = "4")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub location: Vec<Location>,
    #[prost(message, repeated, tag = "5")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub function: Vec<Function>,
    #[prost(string, repeated, tag = "6")]
    #[serde(
        rename = "stringTable",
        alias = "string_table",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub string_table: Vec<String>,
    #[prost(int64, tag = "7")]
    #[serde(
        rename = "dropFrames",
        alias = "drop_frames",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub drop_frames: i64,
    #[prost(int64, tag = "8")]
    #[serde(
        rename = "keepFrames",
        alias = "keep_frames",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub keep_frames: i64,
    #[prost(int64, tag = "9")]
    #[serde(
        rename = "timeNanos",
        alias = "time_nanos",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub time_nanos: i64,
    #[prost(int64, tag = "10")]
    #[serde(
        rename = "durationNanos",
        alias = "duration_nanos",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub duration_nanos: i64,
    #[prost(message, optional, tag = "11")]
    #[serde(
        rename = "periodType",
        alias = "period_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub period_type: Option<ValueType>,
    #[prost(int64, tag = "12")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub period: i64,
    #[prost(int64, repeated, tag = "13")]
    #[serde(
        serialize_with = "crate::int64::ser_vec",
        deserialize_with = "crate::int64::de_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub comment: Vec<i64>,
    #[prost(int64, tag = "14")]
    #[serde(
        rename = "defaultSampleType",
        alias = "default_sample_type",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub default_sample_type: i64,
}

/// A value's type and unit, both string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct ValueType {
    #[prost(int64, tag = "1")]
    #[serde(
        rename = "type",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub r#type: i64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub unit: i64,
}

/// One stack (leaf first) with one value per sample type.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Sample {
    #[prost(uint64, repeated, tag = "1")]
    #[serde(
        rename = "locationId",
        alias = "location_id",
        serialize_with = "crate::uint64::ser_vec",
        deserialize_with = "crate::uint64::de_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub location_id: Vec<u64>,
    #[prost(int64, repeated, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser_vec",
        deserialize_with = "crate::int64::de_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub value: Vec<i64>,
    #[prost(message, repeated, tag = "3")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub label: Vec<Label>,
}

/// A sample label: a string value (`str`) or a number (`num`).
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Label {
    #[prost(int64, tag = "1")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub key: i64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub str: i64,
    #[prost(int64, tag = "3")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub num: i64,
    #[prost(int64, tag = "4")]
    #[serde(
        rename = "numUnit",
        alias = "num_unit",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub num_unit: i64,
}

/// A mapped binary.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Mapping {
    #[prost(uint64, tag = "1")]
    #[serde(
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    #[serde(
        rename = "memoryStart",
        alias = "memory_start",
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub memory_start: u64,
    #[prost(uint64, tag = "3")]
    #[serde(
        rename = "memoryLimit",
        alias = "memory_limit",
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub memory_limit: u64,
    #[prost(uint64, tag = "4")]
    #[serde(
        rename = "fileOffset",
        alias = "file_offset",
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub file_offset: u64,
    #[prost(int64, tag = "5")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub filename: i64,
    #[prost(int64, tag = "6")]
    #[serde(
        rename = "buildId",
        alias = "build_id",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub build_id: i64,
    #[prost(bool, tag = "7")]
    #[serde(
        rename = "hasFunctions",
        alias = "has_functions",
        skip_serializing_if = "is_false"
    )]
    pub has_functions: bool,
    #[prost(bool, tag = "8")]
    #[serde(
        rename = "hasFilenames",
        alias = "has_filenames",
        skip_serializing_if = "is_false"
    )]
    pub has_filenames: bool,
    #[prost(bool, tag = "9")]
    #[serde(
        rename = "hasLineNumbers",
        alias = "has_line_numbers",
        skip_serializing_if = "is_false"
    )]
    pub has_line_numbers: bool,
    #[prost(bool, tag = "10")]
    #[serde(
        rename = "hasInlineFrames",
        alias = "has_inline_frames",
        skip_serializing_if = "is_false"
    )]
    pub has_inline_frames: bool,
}

/// A program location, with its lines innermost first.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Location {
    #[prost(uint64, tag = "1")]
    #[serde(
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    #[serde(
        rename = "mappingId",
        alias = "mapping_id",
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub mapping_id: u64,
    #[prost(uint64, tag = "3")]
    #[serde(
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub address: u64,
    #[prost(message, repeated, tag = "4")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub line: Vec<Line>,
    #[prost(bool, tag = "5")]
    #[serde(
        rename = "isFolded",
        alias = "is_folded",
        skip_serializing_if = "is_false"
    )]
    pub is_folded: bool,
}

/// One line of a location.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Line {
    #[prost(uint64, tag = "1")]
    #[serde(
        rename = "functionId",
        alias = "function_id",
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub function_id: u64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub line: i64,
}

/// A function; its names and file are string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Function {
    #[prost(uint64, tag = "1")]
    #[serde(
        serialize_with = "crate::uint64::ser",
        deserialize_with = "crate::uint64::de",
        skip_serializing_if = "crate::uint64::is_zero"
    )]
    pub id: u64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub name: i64,
    #[prost(int64, tag = "3")]
    #[serde(
        rename = "systemName",
        alias = "system_name",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub system_name: i64,
    #[prost(int64, tag = "4")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub filename: i64,
    #[prost(int64, tag = "5")]
    #[serde(
        rename = "startLine",
        alias = "start_line",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub start_line: i64,
}
