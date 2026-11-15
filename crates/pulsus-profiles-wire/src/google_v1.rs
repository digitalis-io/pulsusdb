//! `google.v1`, the pprof profile message, with the field numbers and types
//! of `proto/google/v1/profile.proto`. String fields other than
//! `string_table` are indices into `string_table`.

/// A profile: its sample types, samples, and the tables they index.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Profile {
    pub sample_type: Vec<ValueType>,
    pub sample: Vec<Sample>,
    pub mapping: Vec<Mapping>,
    pub location: Vec<Location>,
    pub function: Vec<Function>,
    pub string_table: Vec<String>,
    pub drop_frames: i64,
    pub keep_frames: i64,
    pub time_nanos: i64,
    pub duration_nanos: i64,
    pub period_type: Option<ValueType>,
    pub period: i64,
    pub comment: Vec<i64>,
    pub default_sample_type: i64,
}

/// A value's type and unit, both string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ValueType {
    pub r#type: i64,
    pub unit: i64,
}

/// One stack (leaf first) with one value per sample type.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Sample {
    pub location_id: Vec<u64>,
    pub value: Vec<i64>,
    pub label: Vec<Label>,
}

/// A sample label: a string value (`str`) or a number (`num`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Label {
    pub key: i64,
    pub str: i64,
    pub num: i64,
    pub num_unit: i64,
}

/// A mapped binary.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Mapping {
    pub id: u64,
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: i64,
    pub build_id: i64,
    pub has_functions: bool,
    pub has_filenames: bool,
    pub has_line_numbers: bool,
    pub has_inline_frames: bool,
}

/// A program location, with its lines innermost first.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Location {
    pub id: u64,
    pub mapping_id: u64,
    pub address: u64,
    pub line: Vec<Line>,
    pub is_folded: bool,
}

/// One line of a location.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Line {
    pub function_id: u64,
    pub line: i64,
}

/// A function; its names and file are string-table indices.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Function {
    pub id: u64,
    pub name: i64,
    pub system_name: i64,
    pub filename: i64,
    pub start_line: i64,
}

// Test-first stub: every message encodes to nothing and decodes to its
// default. Replaced by the derived codec in the next commit.
macro_rules! stub_message {
    ($($t:ty),*) => {$(
        impl ::prost::Message for $t {
            fn encode_raw(&self, _buf: &mut impl ::prost::bytes::BufMut) {}
            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::WireType,
                buf: &mut impl ::prost::bytes::Buf,
                ctx: ::prost::encoding::DecodeContext,
            ) -> Result<(), ::prost::DecodeError> {
                ::prost::encoding::skip_field(wire_type, tag, buf, ctx)
            }
            fn encoded_len(&self) -> usize {
                0
            }
            fn clear(&mut self) {}
        }
    )*};
}
stub_message!(
    Profile, ValueType, Sample, Label, Mapping, Location, Line, Function
);
