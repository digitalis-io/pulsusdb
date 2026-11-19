//! The seam between the pprof model (`crate::protocols::profile_model`) and
//! the profile writer: the input a receiver hands the model, the rows the
//! model returns, and [`ProfileSink`], which takes them. Pure data and a
//! trait, no I/O — the traces split (`ingest/traces.rs`).
//!
//! One [`ParsedProfileLanding`] is one push. Every row kind is a vector, and
//! rows name each other by position: an entry names its series, a sample its
//! entry.

use std::sync::Arc;

use pulsus_model::{Fingerprint, Tenant};
use pulsus_profiles_wire::google_v1::Profile;

use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};

/// The boundary a profiles receiver hands a parsed push across, the shape of
/// [`crate::ingest::traces::TraceSink`]: admission only. `landing` is parsed
/// in the receiver, so a [`ProfileRefusal`] leaves through the route's own
/// error writer. `tenant` is the request's `X-Scope-OrgID`.
pub trait ProfileSink: Send + Sync {
    /// Admits `landing` for async-mode requests: the caller responds once
    /// this returns `Ok`.
    fn admit(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal>;

    /// Admits `landing` for sync-mode requests: the returned [`FlushWait`]
    /// resolves once the landing insert has committed or failed. For a
    /// suppressed push it resolves to the original push's outcome.
    fn admit_flush(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal>;
}

/// One profile of a push, as a receiver decoded it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileInput {
    /// The push's series labels, verbatim.
    pub labels: Vec<(String, String)>,
    pub profile: Profile,
    /// The profile's size as decoded, for the size check.
    pub decompressed_bytes: u64,
    /// The push's profile id when it is a UUID, else `None`.
    pub id: Option<[u8; 16]>,
    pub annotations: Vec<(String, String)>,
    /// The language the sender names for the profile (OTLP's
    /// `telemetry.sdk.language`); the model takes it over the one it would
    /// detect from the symbols. `None` when the sender names none.
    pub language: Option<String>,
}

/// The rows one push becomes. `stacks` and `locations` are distinct across
/// the whole push, in first-seen order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParsedProfileLanding {
    pub series: Vec<LandingSeries>,
    pub entries: Vec<LandingEntry>,
    pub samples: Vec<LandingSample>,
    pub stacks: Vec<LandingStack>,
    pub locations: Vec<LandingLocation>,
}

impl ParsedProfileLanding {
    /// Rows of every kind.
    pub fn total_rows(&self) -> u64 {
        (self.series.len()
            + self.entries.len()
            + self.samples.len()
            + self.stacks.len()
            + self.locations.len()) as u64
    }

    /// `true` when the push has no entries, so nothing is stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One series of one sample type.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingSeries {
    /// `<__name__>:<type>:<unit>:<period type>:<period unit>`.
    pub type_id: Arc<str>,
    /// The label set's `service_name`.
    pub service: Arc<str>,
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    /// `true` when the profile carried no time and the receive time was used.
    pub time_defaulted: bool,
    /// The label set's canonical JSON.
    pub labels: String,
}

/// One profile of one sample type: the series it belongs to and its total.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingEntry {
    /// Index into [`ParsedProfileLanding::series`].
    pub series: u32,
    /// Position of the entry's [`ProfileInput`] in the call's argument.
    /// Entries of one input share one profile id.
    pub input: u32,
    /// The input's id; `None` when the push carried none, and the writer
    /// mints one per `input`.
    pub profile_id: Option<[u8; 16]>,
    pub total: i64,
    pub duration_ns: i64,
    /// Issue #828: the profile's pprof `period`.
    pub period: i64,
    pub annotation_keys: Vec<String>,
    pub annotation_values: Vec<String>,
}

/// One stack's value within one entry.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingSample {
    /// Index into [`ParsedProfileLanding::entries`].
    pub entry: u32,
    pub stack_id: u128,
    pub value: i64,
    pub span_id: u64,
    pub trace_id: [u8; 16],
}

/// One distinct stack: its frames root first, and its locations leaf first.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingStack {
    pub stack_id: u128,
    /// Parallel to `names`: each frame's id, hashed over its parent's.
    pub node_ids: Vec<u128>,
    /// Function names, root first.
    pub names: Vec<Arc<str>>,
    /// Location ids, leaf first.
    pub location_ids: Vec<u128>,
}

/// One distinct location, with its lines in pprof order (index 0 is the
/// innermost frame).
#[derive(Debug, Clone, PartialEq)]
pub struct LandingLocation {
    pub location_id: u128,
    pub mapping_file: Arc<str>,
    pub mapping_build_id: Arc<str>,
    pub mapping_start: u64,
    pub mapping_limit: u64,
    pub mapping_offset: u64,
    /// Issue #828: the mapping's `has_functions`, `has_filenames`,
    /// `has_line_numbers` and `has_inline_frames` as bits 0–3; 0 without a
    /// mapping.
    pub mapping_flags: u8,
    pub address: u64,
    pub line_functions: Vec<Arc<str>>,
    pub line_system_names: Vec<Arc<str>>,
    pub line_files: Vec<Arc<str>>,
    pub line_start_lines: Vec<i64>,
    pub line_numbers: Vec<i64>,
}

/// Why a push was refused whole. Receivers answer `400` /
/// `invalid_argument` with `message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileRefusal {
    pub message: String,
}
