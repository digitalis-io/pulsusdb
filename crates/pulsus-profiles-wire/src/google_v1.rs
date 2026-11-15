//! `google.v1`, the pprof profile message, with the field numbers and types
//! of `proto/google/v1/profile.proto`. String fields other than
//! `string_table` are indices into `string_table`.

/// A profile: its sample types, samples, and the tables they index.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Profile {
    #[prost(message, repeated, tag = "1")]
    pub sample_type: Vec<ValueType>,
    #[prost(message, repeated, tag = "2")]
    pub sample: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub mapping: Vec<Mapping>,
    #[prost(message, repeated, tag = "4")]
    pub location: Vec<Location>,
    #[prost(message, repeated, tag = "5")]
    pub function: Vec<Function>,
    #[prost(string, repeated, tag = "6")]
    pub string_table: Vec<String>,
    #[prost(int64, tag = "7")]
    pub drop_frames: i64,
    #[prost(int64, tag = "8")]
    pub keep_frames: i64,
    #[prost(int64, tag = "9")]
    pub time_nanos: i64,
    #[prost(int64, tag = "10")]
    pub duration_nanos: i64,
    #[prost(message, optional, tag = "11")]
    pub period_type: Option<ValueType>,
    #[prost(int64, tag = "12")]
    pub period: i64,
    #[prost(int64, repeated, tag = "13")]
    pub comment: Vec<i64>,
    #[prost(int64, tag = "14")]
    pub default_sample_type: i64,
}

/// A value's type and unit, both string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message)]
pub struct ValueType {
    #[prost(int64, tag = "1")]
    pub r#type: i64,
    #[prost(int64, tag = "2")]
    pub unit: i64,
}

/// One stack (leaf first) with one value per sample type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(uint64, repeated, tag = "1")]
    pub location_id: Vec<u64>,
    #[prost(int64, repeated, tag = "2")]
    pub value: Vec<i64>,
    #[prost(message, repeated, tag = "3")]
    pub label: Vec<Label>,
}

/// A sample label: a string value (`str`) or a number (`num`).
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message)]
pub struct Label {
    #[prost(int64, tag = "1")]
    pub key: i64,
    #[prost(int64, tag = "2")]
    pub str: i64,
    #[prost(int64, tag = "3")]
    pub num: i64,
    #[prost(int64, tag = "4")]
    pub num_unit: i64,
}

/// A mapped binary.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message)]
pub struct Mapping {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub memory_start: u64,
    #[prost(uint64, tag = "3")]
    pub memory_limit: u64,
    #[prost(uint64, tag = "4")]
    pub file_offset: u64,
    #[prost(int64, tag = "5")]
    pub filename: i64,
    #[prost(int64, tag = "6")]
    pub build_id: i64,
    #[prost(bool, tag = "7")]
    pub has_functions: bool,
    #[prost(bool, tag = "8")]
    pub has_filenames: bool,
    #[prost(bool, tag = "9")]
    pub has_line_numbers: bool,
    #[prost(bool, tag = "10")]
    pub has_inline_frames: bool,
}

/// A program location, with its lines innermost first.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Location {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub mapping_id: u64,
    #[prost(uint64, tag = "3")]
    pub address: u64,
    #[prost(message, repeated, tag = "4")]
    pub line: Vec<Line>,
    #[prost(bool, tag = "5")]
    pub is_folded: bool,
}

/// One line of a location.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message)]
pub struct Line {
    #[prost(uint64, tag = "1")]
    pub function_id: u64,
    #[prost(int64, tag = "2")]
    pub line: i64,
}

/// A function; its names and file are string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, ::prost::Message)]
pub struct Function {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(int64, tag = "2")]
    pub name: i64,
    #[prost(int64, tag = "3")]
    pub system_name: i64,
    #[prost(int64, tag = "4")]
    pub filename: i64,
    #[prost(int64, tag = "5")]
    pub start_line: i64,
}
