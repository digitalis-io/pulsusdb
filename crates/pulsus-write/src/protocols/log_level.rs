//! Ingest-time log-level detection (issue #483): the rule that decides the
//! `detected_level` structured-metadata pair every log entry carries.
//!
//! Pure functions over `(stream labels, the entry's RESOLVED structured
//! metadata, the line, the raw OTLP severity number)`. No I/O, no clock, no
//! allocation on the common path. Called from the three log-ingest sites —
//! `loki_push::parse_protobuf`, `loki_push::parse_json` and
//! `otlp_logs::parse` — never from the shared canonicalization seam
//! [`crate::protocols::loki_push::structured_metadata_json`], which has
//! neither the stream labels nor the line.
//!
//! # The two views of one entry's metadata, and why they differ
//!
//! **The rule reads the RESOLVED pair list** — names already canonicalized,
//! empty-valued pairs already dropped, i.e. the output of
//! [`pulsus_model::resolve_structured_metadata`]. The reference runs its
//! detector on `normalizedBuilder.Labels()`, the builder output, not on the
//! pairs as the client sent them (`pkg/distributor/distributor.go:697-728 @
//! v3.7.4`). Measured against the pinned reference build: pushing
//! `{"detected.level":"WARN"}` with the line `level=info msg=x` stores
//! `warn` (case d1 — the name is sanitized BEFORE the rule sees it), while
//! `{"log.level":"error"}` stores `unknown` (case d2 — after sanitization
//! the name is `log_level`, and the allowed list carries `log.level`, not
//! `log_level`); and `{"detected_level":""}` or `{"level":""}` beside the
//! line `level=error msg=x` both store `error` (cases e1/e2 — an
//! empty-valued pair is deleted before the rule runs).
//!
//! **The byte cap excludes the pair by its RAW name**, because the
//! reference's `ValidateEntry` runs on the entry as the client sent it,
//! ahead of the builder (`pkg/util/entry_size.go:23-33 @ v3.7.4`,
//! `ExcludedStructuredMetadataLabels`). Measured: a 200 000-byte
//! `detected_level` value is accepted (case c3) and the same value under
//! `detected.level` is rejected (case c4). That exclusion therefore lives at
//! the caller's byte charge in
//! [`crate::protocols::loki_push::canonical_structured_metadata`], on the raw
//! name, and NOT here.
//!
//! Two views of the same data, each separately measured. A reader who
//! assumes one view is used everywhere will be wrong about one of them.
//!
//! # What is gated, and what is only written down
//!
//! Several comments in this module separate what was OBSERVED from the
//! reference from what was INFERRED about the mechanism behind it. That
//! distinction is prose. **No test enforces it, and none can** — a comment
//! cannot be made to fail.
//!
//! What IS enforced is the answer. `tests/fixtures/detected_level/
//! reference_cases.json` holds the reference's captured answer for each of
//! the 80 named inputs, `tests/detected_level_reference_cases.rs` asserts
//! every one of them against this code, and a live replay leg re-pushes them
//! to a running reference so the file cannot drift into a transcription of
//! our own output. Those rows fail on a wrong answer however any comment
//! here is worded, and they are the reason a mis-stated mechanism is a
//! documentation defect rather than a correctness one.
//!
//! So: correct the prose when it overstates, and do not expect a gate to
//! catch it. The gate is the table.
//!
//! # Precedence
//!
//! `extractLogLevel` (`pkg/distributor/field_detection.go:96-124 @ v3.7.4`):
//!
//! 1. The resolved pairs already carry `detected_level` — normalize that
//!    value IN PLACE and add nothing. "In place" means the FIRST such pair
//!    in the view's own order, which on OTLP is not always the pair
//!    PulsusDB stores; see [`LevelOutcome::LeaveStored`] and [`OtlpView`].
//! 2. Else the first [`ALLOWED_LEVEL_FIELDS`] entry present in the STREAM
//!    labels, normalized.
//! 3. Else the same list over the entry's other resolved metadata. On OTLP
//!    that metadata has three sources and their order is measured, not
//!    assumed: record attributes, then the record's severity text, then the
//!    scope ([`OtlpView`]).
//! 4. Else detect from the entry: an OTLP severity number maps by the OTLP
//!    bands; otherwise the line is JSON- or logfmt-parsed for those field
//!    names, and on no match the whole line is scanned for the earliest
//!    word-bounded level word.
//! 5. Every entry gets a value, `unknown` included.

use std::borrow::Cow;

use pulsus_model::LabelSet;

/// The structured-metadata name the level is stored under
/// (`pkg/util/constants/levels.go:4 @ v3.7.4`).
pub const DETECTED_LEVEL: &str = "detected_level";

/// The fallthrough value (`pkg/util/constants/levels.go:5 @ v3.7.4`). Every
/// entry gets a value; there is no "nothing to add" path once the line has
/// been read.
pub const UNKNOWN: &str = "unknown";

/// The allowed field names, IN PRECEDENCE ORDER
/// (`pkg/validation/limits.go:70-85 @ v3.7.4`,
/// `DefaultAllowedLevelFields`). Separate list entries per spelling — this
/// is NOT case-insensitive matching, which cases lvl11
/// (`{"lEvEl":"information"}` -> `unknown`) and lvl12 (stream label
/// `LeVeL=warning` -> `unknown`) are the only rows to discriminate.
///
/// The ORDER is observable: it decides which field wins when a label set, a
/// metadata set or a logfmt line carries two of them (lvl31, lvl34, lvl35),
/// because the reference walks the LIST and asks whether each name is
/// present (`labelsContainAny`, `field_detection.go:145-152 @ v3.7.4`)
/// rather than walking the data.
pub const ALLOWED_LEVEL_FIELDS: [&str; 14] = [
    "level",
    "LEVEL",
    "Level",
    "log.level",
    "severity",
    "SEVERITY",
    "Severity",
    "SeverityText",
    "lvl",
    "LVL",
    "Lvl",
    "severity_text",
    "Severity_Text",
    "SEVERITY_TEXT",
];

/// Spelling -> level for a FIELD VALUE (`normalizeLogLevel`,
/// `field_detection.go:156-176 @ v3.7.4`, and the identical switch at
/// `:228-244`). An unmatched value is returned UNCHANGED, never mapped to
/// `unknown` (lvl15 stores `banana`, o13 stores `banana`).
///
/// The VALUE column is what a test must pin: a check that the right set of
/// level words appears cannot see two of them swapped.
const VALUE_WORDS: [(&str, &str); 14] = [
    ("trace", "trace"),
    ("trc", "trace"),
    ("debug", "debug"),
    ("dbg", "debug"),
    ("info", "info"),
    ("inf", "info"),
    ("information", "info"),
    ("warn", "warn"),
    ("wrn", "warn"),
    ("warning", "warn"),
    ("error", "error"),
    ("err", "error"),
    ("critical", "critical"),
    ("fatal", "fatal"),
];

/// Word -> level for the whole-line scan (`levelPatterns`,
/// `field_detection.go:41-55 @ v3.7.4`). The EARLIEST word-bounded
/// occurrence wins, so list order is not observable here: with the boundary
/// checks applied no two distinct words in this table can match at the same
/// offset (`err` inside `error` fails the right boundary, `warn` inside
/// `warning` likewise). The VALUE column is what a test must pin.
///
/// `dbug` is deliberately absent — case lvl4, `lvl=dbug entering loop`,
/// answers `unknown`.
const LINE_WORDS: [(&str, &str); 9] = [
    ("trace", "trace"),
    ("debug", "debug"),
    ("fatal", "fatal"),
    ("critical", "critical"),
    ("error", "error"),
    ("err", "error"),
    ("warning", "warn"),
    ("warn", "warn"),
    ("info", "info"),
];

/// The maximum object nesting the JSON field search descends into
/// (`validation.log-level-from-json-max-depth`, default `2`,
/// `pkg/validation/limits.go:349 @ v3.7.4`). The top-level object is depth
/// `0`, so two levels of objects are searched: lvl13
/// (`{"log":{"level":"warn"}}` -> `warn`) is inside the limit and lvl14
/// (`{"a":{"b":{"level":"information"}}}` -> `unknown`) is outside it.
const JSON_MAX_DEPTH: u32 = 2;

/// Whether ingest-time level detection runs at all
/// (`writer.discover_log_levels` / `PULSUS_DISCOVER_LOG_LEVELS`). `Off`
/// gates the WHOLE rule, step 1 included: a client-supplied
/// `detected_level` is then stored exactly as sent, un-normalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelDiscovery {
    On,
    Off,
}

/// What the caller must do to the entry's STORED structured metadata.
///
/// **There is no "add nothing because the level was empty" arm.** The
/// reference has one — `logLevel == ""` returns `ok=false`
/// (`field_detection.go:119-121 @ v3.7.4`) — and it is unreachable for the
/// same reason here as there: the only source of an empty level is
/// normalizing an empty field value, and both views this module reads drop
/// empty values before the rule sees them (the resolved pair list by
/// construction, [`OtlpView`] explicitly). Every other arm ends in the
/// whole-line scan, whose own fallthrough is [`UNKNOWN`], never `""`. An arm
/// that cannot be constructed is a state better left unrepresentable — so
/// someone comparing this enum against the reference's three outcomes will
/// see a difference that is not one.
///
/// [`LevelOutcome::LeaveStored`] is a different thing and IS reachable: it
/// is not "no level", it is "the level went into a pair you do not store".
#[derive(Debug, PartialEq, Eq)]
pub enum LevelOutcome<'a> {
    /// The STORED pairs already carry `detected_level`, and it is the pair
    /// the rule read. Replace its value with this one and add nothing.
    NormalizeExisting(Cow<'a, str>),
    /// The rule read a `detected_level` pair the caller does NOT store, and
    /// the caller's own stored pair is to be left exactly as it is. Change
    /// nothing.
    ///
    /// Reachable on OTLP only. Measured: with `detected_level` present in
    /// both a record attribute and a scope attribute, the reference answers
    /// with the SCOPE attribute's value, un-normalized (cases p10, p11).
    /// Why it does — a rewrite of the record's copy that leaves the scope's
    /// alone — is an inference from its source and is not visible in the
    /// response, which carries one pair under that name. See [`OtlpView`]
    /// for both halves.
    LeaveStored,
    /// Append a `detected_level` pair with this value.
    Append(Cow<'a, str>),
}

impl LevelOutcome<'_> {
    /// The level value, or `None` for [`LevelOutcome::LeaveStored`], which
    /// carries no value because the caller writes none.
    pub fn value(&self) -> Option<&str> {
        match self {
            LevelOutcome::NormalizeExisting(v) | LevelOutcome::Append(v) => Some(v.as_ref()),
            LevelOutcome::LeaveStored => None,
        }
    }

    /// Detaches the outcome from the inputs it borrows, so the caller can
    /// mutate the very pair list the rule read. The value becomes a `String`
    /// on the way into the stored pair list either way, so this costs
    /// nothing the caller was not already paying.
    pub fn into_owned(self) -> LevelOutcome<'static> {
        match self {
            LevelOutcome::NormalizeExisting(v) => {
                LevelOutcome::NormalizeExisting(Cow::Owned(v.into_owned()))
            }
            LevelOutcome::LeaveStored => LevelOutcome::LeaveStored,
            LevelOutcome::Append(v) => LevelOutcome::Append(Cow::Owned(v.into_owned())),
        }
    }
}

/// The entry's structured metadata AS THE RULE MUST SEE IT: names already
/// canonicalized, empty values already dropped. See this module's header for
/// the two measurements (d1/d2 and e1/e2) that fix this contract.
pub trait MetadataView {
    /// The value stored under `canonical_name`, or `None`. Never returns an
    /// empty value.
    fn get(&self, canonical_name: &str) -> Option<Cow<'_, str>>;

    /// The `detected_level` value among the pairs the caller will actually
    /// STORE, which is not always the same set [`MetadataView::get`] reads.
    /// On the two push transports the two sets are identical; on OTLP the
    /// rule additionally reads the record's own attributes and severity
    /// text, none of which PulsusDB stores (issue #109 placement), so case
    /// p7 — a record attribute named `detected_level` — is a
    /// [`LevelOutcome::Append`] here and an in-place normalization on the
    /// reference. Both store the same one pair with the same value.
    fn stored_level(&self) -> Option<&str>;

    /// Whether the `detected_level` the rule read is the pair the caller
    /// stores.
    ///
    /// **Observed:** `false` exactly when the reference's answer is a
    /// `detected_level` the caller does not store — on OTLP, when the name
    /// arrives as a record attribute beside a scope attribute, the answer is
    /// the SCOPE attribute's value verbatim (cases p10, p11), so the
    /// caller's own pair must be left as it is:
    /// [`LevelOutcome::LeaveStored`].
    ///
    /// **Inferred:** that the reference reaches that answer by rewriting the
    /// record's copy and leaving the scope's alone. Not visible in its
    /// response — see [`OtlpView`]. The name of this method describes that
    /// reading; what it must RETURN is fixed by the observed answers alone.
    fn first_level_is_stored(&self) -> bool;
}

/// Resolved-pair view for the two push transports: the same list the caller
/// stores, so [`MetadataView::get`] and [`MetadataView::stored_level`] read
/// one set.
pub struct PairsView<'a>(pub &'a [(String, String)]);

impl MetadataView for PairsView<'_> {
    fn get(&self, canonical_name: &str) -> Option<Cow<'_, str>> {
        self.0
            .iter()
            .find(|(name, value)| name == canonical_name && !value.is_empty())
            .map(|(_, value)| Cow::Borrowed(value.as_str()))
    }

    fn stored_level(&self) -> Option<&str> {
        self.0
            .iter()
            .find(|(name, _)| name == DETECTED_LEVEL)
            .map(|(_, value)| value.as_str())
    }

    /// Always: this view IS the stored list, so the pair the rule reads is
    /// the pair the caller writes.
    fn first_level_is_stored(&self) -> bool {
        true
    }
}

/// A lazily-read attribute list. The rule asks for at most fifteen names and
/// stops at the first hit, so an OTLP record's attributes are never rendered
/// wholesale just to look for a level.
pub trait AttributeLookup {
    /// The value of the attribute whose key canonicalizes to
    /// `canonical_name`, in wire order, or `None`.
    fn get(&self, canonical_name: &str) -> Option<Cow<'_, str>>;
}

/// An [`AttributeLookup`] over nothing, for a caller with no record
/// attributes to offer.
pub struct NoAttributes;

impl AttributeLookup for NoAttributes {
    fn get(&self, _canonical_name: &str) -> Option<Cow<'_, str>> {
        None
    }
}

/// The OTLP view: the record's own attributes and severity text, plus the
/// scope's RESOLVED structured-metadata pairs — the last of which is the
/// only part of this view PulsusDB stores.
///
/// PulsusDB stores neither the severity text nor the record attributes
/// (issue #109 places record attributes outside storage, and severity is a
/// first-class `i8` column). They are fed in because the reference's
/// receiver puts both into structured metadata, where the rule reads them
/// (`pkg/loghttp/push/otlp.go:482-547 @ v3.7.4`): o9 answers `warn` from the
/// severity TEXT while the severity number says `error`, o13 passes `banana`
/// through, and p3/p4/p7 answer from a record attribute.
///
/// # Which source answers when a name arrives in two of them
///
/// **Observed.** When one allowed field name arrives in both a record
/// attribute and a scope attribute, the reference's answer is the RECORD
/// attribute's value (p8, p9). When that name is `detected_level`, its
/// answer is the SCOPE attribute's value, verbatim and un-normalized (p10,
/// p11). Those four answers are the whole of what the probes show, and they
/// are what this view is built to reproduce.
///
/// **Inferred, from the reference's source.** That it holds one ordered
/// per-entry metadata slice — record attributes, then the severity fields,
/// then the resource and scope attributes (`otlp.go:482-547`, then
/// `:400-404`) — and looks up the first entry under a name. This module
/// implements the same precedence (record attributes, then the record's
/// severity text, then the scope) because it reproduces the four observed
/// answers, not because the slice order is itself observable.
///
/// **The four answers were measured, not read.** Plan v2 specified the
/// opposite and marked the question unmeasured; four probes against the
/// pinned reference build settled it and the plan's choice was wrong:
///
/// | probe | scope attribute | record attribute | reference answered |
/// |---|---|---|---|
/// | p8  | `level=critical`        | `level=warn`             | `warn` |
/// | p9  | `level=warn`            | `level=critical`         | `critical` |
/// | p10 | `detected_level=banana` | `detected_level=WARN`    | `banana` |
/// | p11 | `detected_level=WARN`   | `detected_level=banana`  | `WARN` |
///
/// p8 and p9 are the same probe with the values swapped, so the answer
/// follows the RECORD in both assignments and cannot be a coincidence of
/// which value happened to sit where.
///
/// p10 and p11 look inverted and are not. Keep the two halves apart:
///
/// **Observed, on the wire.** Each response carries ONE pair named
/// `detected_level`, and its value is the SCOPE attribute's, verbatim —
/// `banana` where the record said `WARN`, `WARN` where the record said
/// `banana`. Not normalized, in either direction. Those two answers are the
/// whole of what the probes show.
///
/// **Inferred, from the reference's source, and NOT visible in that
/// response.** The mechanism that would produce it: both pairs reach step 1
/// (`field_detection.go:97-107 @ v3.7.4` scans the entry's metadata for the
/// name), it rewrites the FIRST match and returns, and the first is the
/// record's because record attributes are appended to the entry's metadata
/// ahead of the scope's (`pkg/loghttp/push/otlp.go:482-547`, then
/// `:400-404`); the scope's copy is therefore untouched, and a consumer of
/// the JSON object sees the last pair under a repeated name. **The response
/// carries one such pair, not two, so the pair count and the rewrite are
/// not observable in it.** The reading is consistent with the two answers
/// and is offered as a reading.
///
/// What PulsusDB does rests on the OBSERVED half alone: our column is
/// key-unique, we store only the scope pair, and storing it verbatim
/// reproduces both answers — [`LevelOutcome::LeaveStored`]. If the inferred
/// mechanism is ever shown to be wrong, the two captured answers are
/// unaffected and so is this code.
pub struct OtlpView<'a, A> {
    /// The record's own attributes.
    pub record_attributes: A,
    /// The record's `severity_text`; empty when the record carried none.
    pub severity_text: &'a str,
    /// The scope's resolved pairs, canonical names, no empty values. The
    /// only member of this view the caller stores.
    pub scope_pairs: &'a [(String, String)],
}

impl<A: AttributeLookup> MetadataView for OtlpView<'_, A> {
    fn get(&self, canonical_name: &str) -> Option<Cow<'_, str>> {
        // The reference's builder deletes an empty-valued pair before the
        // rule runs, so an empty record attribute is not a value here
        // either.
        if let Some(value) = self
            .record_attributes
            .get(canonical_name)
            .filter(|value| !value.is_empty())
        {
            return Some(value);
        }
        if canonical_name == SEVERITY_TEXT && !self.severity_text.is_empty() {
            return Some(Cow::Borrowed(self.severity_text));
        }
        self.scope_pairs
            .iter()
            .find(|(name, value)| name == canonical_name && !value.is_empty())
            .map(|(_, value)| Cow::Borrowed(value.as_str()))
    }

    fn stored_level(&self) -> Option<&str> {
        self.scope_pairs
            .iter()
            .find(|(name, _)| name == DETECTED_LEVEL)
            .map(|(_, value)| value.as_str())
    }

    fn first_level_is_stored(&self) -> bool {
        // Measured (p10, p11): when a record attribute carries this name,
        // the reference's answer is the SCOPE attribute's value verbatim, so
        // ours must leave the scope pair alone. The ordering account of why
        // is an inference — see [`OtlpView`]. The severity text cannot carry
        // this name.
        self.record_attributes
            .get(DETECTED_LEVEL)
            .is_none_or(|value| value.is_empty())
    }
}

/// The structured-metadata name the OTLP receiver stores a record's
/// `severityText` under (`pkg/loghttp/push/otlp.go:43 @ v3.7.4`). It is also
/// [`ALLOWED_LEVEL_FIELDS`] entry 11, which is what makes the severity text
/// beat the severity number (o9).
pub const SEVERITY_TEXT: &str = "severity_text";

/// Steps 1-5. `otlp_severity_number` is the RAW wire value: `None` on the
/// two push transports, `Some(n)` on OTLP where `0` means the record carried
/// no severity number and the line is read instead (o1, o12), while a value
/// above the fatal band answers `unknown` and never reaches the line (p1,
/// p2). Collapsing the number into the stored `i8` first would merge those
/// two answers, which is why the raw value is carried.
pub fn resolve<'a, M: MetadataView + ?Sized>(
    stream_labels: &'a LabelSet,
    metadata: &'a M,
    line: &'a str,
    otlp_severity_number: Option<i32>,
) -> LevelOutcome<'a> {
    // Step 1. `field_detection.go:97-107 @ v3.7.4` — a pre-existing pair is
    // normalized IN PLACE and nothing is appended beside it. The pair that
    // gets normalized is not always the one the caller stores: on OTLP, a
    // `detected_level` arriving as a record attribute leaves a scope
    // attribute of the same name stored verbatim (measured, p10/p11). See
    // [`LevelOutcome::LeaveStored`] and [`OtlpView`].
    if let Some(existing) = metadata.get(DETECTED_LEVEL) {
        let value = normalize_owned(existing);
        return if metadata.first_level_is_stored() {
            LevelOutcome::NormalizeExisting(value)
        } else if metadata.stored_level().is_some() {
            LevelOutcome::LeaveStored
        } else {
            // The rewritten pair is not stored and there is no stored one to
            // leave alone, so the level has to be written fresh (p7).
            LevelOutcome::Append(value)
        };
    }

    // Step 2, then step 3: the allowed list is walked, and for each name the
    // labels are asked whether they have it — list order, not data order
    // (`labelsContainAny`, `field_detection.go:145-152 @ v3.7.4`).
    //
    // Every step below appends, and that is an invariant rather than a
    // choice: reaching them means step 1 found no `detected_level` anywhere
    // in the view, and both views expose every stored `detected_level`
    // through `get`, so there is none to rewrite.
    for name in ALLOWED_LEVEL_FIELDS {
        if let Some(value) = stream_labels.get(name)
            && !value.is_empty()
        {
            return LevelOutcome::Append(normalize(value));
        }
    }
    for name in ALLOWED_LEVEL_FIELDS {
        if let Some(value) = metadata.get(name) {
            return LevelOutcome::Append(normalize_owned(value));
        }
    }

    // Step 4.
    LevelOutcome::Append(Cow::Borrowed(detect_from_entry(line, otlp_severity_number)))
}

/// Value normalization. An unmatched value is returned UNCHANGED, not mapped
/// to [`UNKNOWN`] (lvl15, o13).
///
/// The comparison is ASCII case-insensitive. The reference uses
/// `bytes.EqualFold`, which additionally folds non-ASCII simple case pairs;
/// the two differ only for a field VALUE carrying a non-ASCII spelling of a
/// level word, which no measured case exercises. The whole-line scan's
/// lowercasing is a separate rule and is Unicode-aware — see
/// [`lower_first_char`].
pub fn normalize(value: &str) -> Cow<'_, str> {
    match normalized_level(value) {
        Some(level) => Cow::Borrowed(level),
        None => Cow::Borrowed(value),
    }
}

/// [`normalize`] over an already-owned value, preserving the allocation
/// rather than copying it again.
fn normalize_owned(value: Cow<'_, str>) -> Cow<'_, str> {
    match normalized_level(value.as_ref()) {
        Some(level) => Cow::Borrowed(level),
        None => value,
    }
}

/// The one mapping table behind [`normalize`]: `None` means "not a level
/// word", which the callers render as "leave the value alone".
fn normalized_level(value: &str) -> Option<&'static str> {
    VALUE_WORDS
        .iter()
        .find(|(spelling, _)| value.eq_ignore_ascii_case(spelling))
        .map(|(_, level)| *level)
}

/// Step 4 (`detectLogLevelFromLogEntry`, `field_detection.go:179-203 @
/// v3.7.4`): the OTLP severity number if the record carried one, else the
/// line.
fn detect_from_entry(line: &str, otlp_severity_number: Option<i32>) -> &'static str {
    // `0` is `SeverityNumberUnspecified`; the reference's receiver does not
    // put it in structured metadata at all, so the detector's
    // `Get("severity_number")` comes back empty and the line is read (o1,
    // o12). Every other value maps by the OTLP bands and the line is never
    // read (o11).
    if let Some(number) = otlp_severity_number
        && number != 0
    {
        return severity_band(number);
    }
    detect_from_line(line)
}

/// The OTLP severity bands (`field_detection.go:182-201 @ v3.7.4`, against
/// `plog.SeverityNumber*4`). Above the fatal band the answer is `unknown`
/// and the line is never consulted (p1, p2).
fn severity_band(number: i32) -> &'static str {
    if number <= 4 {
        "trace"
    } else if number <= 8 {
        "debug"
    } else if number <= 12 {
        "info"
    } else if number <= 16 {
        "warn"
    } else if number <= 20 {
        "error"
    } else if number <= 24 {
        "fatal"
    } else {
        UNKNOWN
    }
}

/// `extractLogLevelFromLogLine` (`field_detection.go:218-247 @ v3.7.4`): a
/// JSON line is searched for an allowed field, a logfmt line likewise, and
/// anything else — or a value that is not a level word — falls through to
/// the whole-line scan.
fn detect_from_line(line: &str) -> &'static str {
    let extracted = if is_json(line) {
        json_field_value(line)
    } else if is_logfmt(line) {
        logfmt_field_value(line)
    } else {
        return detect_from_line_scan(line);
    };
    match extracted.as_deref().and_then(normalized_level) {
        Some(level) => level,
        None => detect_from_line_scan(line),
    }
}

/// `isJSON` (`field_detection.go:326-346 @ v3.7.4`): the first non-space
/// CHARACTER is `{` and the last non-space BYTE is `}`. The reference reads
/// the trailing end byte-wise and widens each byte to a rune, so the two
/// ends are not scanned the same way; this reproduces that asymmetry rather
/// than tidying it.
fn is_json(line: &str) -> bool {
    let first = line.chars().find(|c| !is_go_space(*c));
    let last = line
        .as_bytes()
        .iter()
        .rev()
        .find(|b| !is_go_space(**b as char));
    first == Some('{') && last == Some(&b'}')
}

/// Go's `unicode.IsSpace` over the Latin-1 range, which is all either
/// `isJSON` end can produce.
fn is_go_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{85}' | '\u{a0}'
    ) || (c > '\u{ff}' && c.is_whitespace())
}

/// `isLogFmt` (`field_detection.go:311-317 @ v3.7.4`): a non-empty line
/// containing `=`.
fn is_logfmt(line: &str) -> bool {
    !line.is_empty() && line.as_bytes().contains(&b'=')
}

/// `getLevelUsingJSONParser` (`field_detection.go:284-309 @ v3.7.4`):
/// depth-first over the object in DOCUMENT order, taking the first STRING
/// value whose key is an exact [`ALLOWED_LEVEL_FIELDS`] entry, descending
/// into nested objects up to [`JSON_MAX_DEPTH`].
///
/// Document order is what separates the JSON step from the logfmt step:
/// lvl32 (`{"severity":"critical","level":"error"}` -> `critical`) and lvl33
/// (the same two members swapped -> `error`) answer differently, while the
/// logfmt line lvl31 answers by LIST order regardless of where the fields
/// sit.
fn json_field_value(line: &str) -> Option<Cow<'_, str>> {
    let mut found: Option<Cow<'_, str>> = None;
    let mut de = serde_json::Deserializer::from_str(line);
    // The result is whatever was found before the walk stopped, exactly as
    // the reference discards its parser's error and returns `result`. A
    // malformed tail therefore does not erase a level already seen, and a
    // line that is not an object at all simply finds nothing.
    let _ = serde::de::DeserializeSeed::deserialize(
        ObjectSeed {
            depth: 0,
            found: &mut found,
        },
        &mut de,
    );
    found
}

/// One object level of [`json_field_value`]'s walk.
struct ObjectSeed<'f, 'de> {
    depth: u32,
    found: &'f mut Option<Cow<'de, str>>,
}

impl<'de> serde::de::DeserializeSeed<'de> for ObjectSeed<'_, 'de> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> serde::de::Visitor<'de> for ObjectSeed<'_, 'de> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            map.next_value_seed(MemberSeed {
                key: &key,
                depth: self.depth,
                found: self.found,
            })?;
            if self.found.is_some() {
                // The reference stops the whole walk on the first hit by
                // returning a sentinel error from its callback; an `Err`
                // here does the same, and `json_field_value` reads the
                // value out of `found` rather than out of the result.
                return Err(serde::de::Error::custom("level found"));
            }
        }
        Ok(())
    }
}

/// One `(key, value)` member: a string value may answer, an object value is
/// descended into, everything else is skipped.
struct MemberSeed<'f, 'k, 'de> {
    key: &'k str,
    depth: u32,
    found: &'f mut Option<Cow<'de, str>>,
}

impl<'de> serde::de::DeserializeSeed<'de> for MemberSeed<'_, '_, 'de> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for MemberSeed<'_, '_, 'de> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        // The escaped-string arm: `serde_json` had to decode into scratch,
        // so the value cannot be borrowed from the line.
        if ALLOWED_LEVEL_FIELDS.contains(&self.key) {
            *self.found = Some(Cow::Owned(value.to_string()));
        }
        Ok(())
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<(), E> {
        // The common arm: an unescaped JSON string is a slice of the line
        // itself, so the level check costs no allocation.
        if ALLOWED_LEVEL_FIELDS.contains(&self.key) {
            *self.found = Some(Cow::Borrowed(value));
        }
        Ok(())
    }

    fn visit_map<A>(self, map: A) -> Result<(), A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // The depth check is charged on ENTRY to the nested object, so the
        // top-level object is depth 0 and `JSON_MAX_DEPTH` object levels are
        // searched in total.
        if self.depth + 1 >= JSON_MAX_DEPTH {
            return drain_map(map);
        }
        serde::de::Visitor::visit_map(
            ObjectSeed {
                depth: self.depth + 1,
                found: self.found,
            },
            map,
        )
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
        Ok(())
    }

    fn visit_bool<E>(self, _v: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E>(self, _v: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E>(self, _v: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E>(self, _v: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E>(self) -> Result<(), E> {
        Ok(())
    }
}

fn drain_map<'de, A>(mut map: A) -> Result<(), A::Error>
where
    A: serde::de::MapAccess<'de>,
{
    while map
        .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
        .is_some()
    {}
    Ok(())
}

/// `getValueUsingLogfmtParser` (`field_detection.go:249-269 @ v3.7.4`): the
/// WHOLE line is scanned and the match with the smallest
/// [`ALLOWED_LEVEL_FIELDS`] index wins, so a `level=` later in the line
/// beats a `severity=` earlier in it (lvl31). Keys are matched case
/// INSENSITIVELY on this path, which is why lvl9 (`SEVERITY=fatal`) does not
/// discriminate the separate-spellings rule.
fn logfmt_field_value(line: &str) -> Option<Cow<'_, str>> {
    let mut best: Option<(usize, Cow<'_, str>)> = None;
    for (key, value) in LogfmtScan::new(line.as_bytes()) {
        let Ok(key) = std::str::from_utf8(key) else {
            continue;
        };
        // The reference walks the hint list per key and keeps the smallest
        // index seen so far; the first hint that matches a given key already
        // has that key's smallest index, so one `position` is the same
        // answer.
        let Some(index) = ALLOWED_LEVEL_FIELDS
            .iter()
            .position(|field| key.eq_ignore_ascii_case(field))
        else {
            continue;
        };
        if best.as_ref().is_none_or(|(b, _)| index < *b) {
            best = Some((index, bytes_to_str(value)));
            if index == 0 {
                // "If the matching hint is the first one, we can stop
                // parsing the rest of the line" (`field_detection.go:263-266
                // @ v3.7.4`).
                return best.map(|(_, v)| v);
            }
        }
    }
    best.map(|(_, v)| v)
}

/// Renders a scanned logfmt value as text, keeping the borrow when the
/// scanner did not have to unescape it.
fn bytes_to_str(value: Cow<'_, [u8]>) -> Cow<'_, str> {
    match value {
        Cow::Borrowed(b) => String::from_utf8_lossy(b),
        Cow::Owned(b) => Cow::Owned(String::from_utf8_lossy(&b).into_owned()),
    }
}

/// A byte-wise logfmt key/value scanner mirroring the reference decoder
/// (`pkg/logql/log/logfmt/decode.go:41-180 @ v3.7.4`, itself adapted from
/// `go-logfmt`): leading bytes `<= ' '` are skipped, a key runs to `=` or to
/// the next byte `<= ' '`, a value is either bare (to the next byte `<= ' '`)
/// or double-quoted, and a syntax error ends the scan for the whole line —
/// it does not resynchronize.
struct LogfmtScan<'a> {
    line: &'a [u8],
    pos: usize,
}

impl<'a> LogfmtScan<'a> {
    fn new(line: &'a [u8]) -> Self {
        LogfmtScan { line, pos: 0 }
    }
}

impl<'a> Iterator for LogfmtScan<'a> {
    type Item = (&'a [u8], Cow<'a, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        // Garbage.
        while self.pos < self.line.len() && self.line[self.pos] <= b' ' {
            self.pos += 1;
        }
        if self.pos >= self.line.len() {
            return None;
        }
        // Key.
        let start = self.pos;
        let mut multibyte = false;
        let mut has_equals = false;
        while self.pos < self.line.len() {
            let c = self.line[self.pos];
            if c == b'=' {
                has_equals = true;
                break;
            }
            if c == b'"' {
                // `unexpectedByte` -> `skip_value`, which returns false and
                // ends the scan.
                return None;
            }
            if c <= b' ' {
                break;
            }
            if c >= 0x80 {
                multibyte = true;
            }
            self.pos += 1;
        }
        let key = &self.line[start..self.pos];
        if key.is_empty() {
            // A bare `=` with no key: `unexpectedByte` -> end of scan.
            return None;
        }
        if multibyte && key.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]) {
            // `invalidKeyError`: the key holds an encoded U+FFFD.
            return None;
        }
        if !has_equals {
            return Some((key, Cow::Borrowed(&[][..])));
        }
        self.pos += 1; // past `=`
        if self.pos >= self.line.len() || self.line[self.pos] <= b' ' {
            return Some((key, Cow::Borrowed(&[][..])));
        }
        if self.line[self.pos] == b'"' {
            return self.quoted_value(key);
        }
        let vstart = self.pos;
        while self.pos < self.line.len() {
            let c = self.line[self.pos];
            if c == b'=' || c == b'"' {
                // `unexpectedByte` -> `skip_value` -> end of scan, and the
                // partial pair is NOT reported.
                return None;
            }
            if c <= b' ' {
                break;
            }
            self.pos += 1;
        }
        Some((key, Cow::Borrowed(&self.line[vstart..self.pos])))
    }
}

impl<'a> LogfmtScan<'a> {
    fn quoted_value(&mut self, key: &'a [u8]) -> Option<(&'a [u8], Cow<'a, [u8]>)> {
        let start = self.pos;
        let mut has_esc = false;
        let mut esc = false;
        let mut i = self.pos + 1;
        while i < self.line.len() {
            let c = self.line[i];
            if esc {
                esc = false;
            } else if c == b'\\' {
                has_esc = true;
                esc = true;
            } else if c == b'"' {
                self.pos = i + 1;
                if has_esc {
                    // The reference unquotes with a copy of Go's JSON string
                    // decoder; `serde_json` is the same grammar. An
                    // unquotable value is `invalidQuote` -> end of scan.
                    let raw = std::str::from_utf8(&self.line[start..self.pos]).ok()?;
                    let decoded: String = serde_json::from_str(raw).ok()?;
                    return Some((key, Cow::Owned(decoded.into_bytes())));
                }
                return Some((key, Cow::Borrowed(&self.line[start + 1..self.pos - 1])));
            }
            i += 1;
        }
        // `untermQuote`: end of scan.
        self.pos = self.line.len();
        None
    }
}

/// `detectLevelFromLogLine` (`field_detection.go:386-401 @ v3.7.4`): the
/// EARLIEST word-bounded occurrence of a [`LINE_WORDS`] entry in the
/// lowercased line, or [`UNKNOWN`].
fn detect_from_line_scan(line: &str) -> &'static str {
    if line.is_ascii() {
        // Zero-allocation path: ASCII lowercasing is byte-for-byte, so both
        // the offsets and the boundary bytes are the same in the original
        // line as in the lowered one.
        return scan_lowered(line.as_bytes(), AsciiFold::Fold);
    }
    let lowered = lower_simple(line);
    scan_lowered(lowered.as_bytes(), AsciiFold::None)
}

/// Whether [`scan_lowered`] must fold ASCII case while comparing, because it
/// was handed the original line rather than a lowered copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsciiFold {
    Fold,
    None,
}

fn scan_lowered(haystack: &[u8], fold: AsciiFold) -> &'static str {
    let mut best_index = haystack.len();
    let mut best = UNKNOWN;
    for (word, level) in LINE_WORDS {
        let Some(pos) = index_of_bounded(haystack, word.as_bytes(), fold) else {
            continue;
        };
        if pos >= best_index {
            continue;
        }
        best_index = pos;
        best = level;
        if pos == 0 {
            break;
        }
    }
    best
}

/// `indexOfBoundedLevel` (`field_detection.go:371-384 @ v3.7.4`).
fn index_of_bounded(haystack: &[u8], needle: &[u8], fold: AsciiFold) -> Option<usize> {
    let mut offset = 0usize;
    while offset + needle.len() <= haystack.len() {
        let window = &haystack[offset..];
        let abs = offset + find_at(window, needle, fold)?;
        if is_left_boundary(haystack, abs.checked_sub(1))
            && is_right_boundary(haystack, Some(abs + needle.len()))
        {
            return Some(abs);
        }
        offset = abs + 1;
    }
    None
}

fn find_at(haystack: &[u8], needle: &[u8], fold: AsciiFold) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| match fold {
        AsciiFold::Fold => w.eq_ignore_ascii_case(needle),
        AsciiFold::None => w == needle,
    })
}

/// `isLeftWordBoundary` (`field_detection.go:349-358 @ v3.7.4`). `:` is
/// deliberately EXCLUDED — `misc:error` is a key/value compound, not a level
/// (lvl16). Out of range is a boundary. Byte comparisons, so a multi-byte
/// neighbour is never a boundary (lvl21).
fn is_left_boundary(s: &[u8], pos: Option<usize>) -> bool {
    let Some(pos) = pos else {
        return true;
    };
    let Some(c) = s.get(pos) else {
        return true;
    };
    matches!(c, b' ' | b'\t' | b'\n' | b'[' | b'(' | b'{' | b'"' | b'=')
}

/// `isRightWordBoundary` (`field_detection.go:360-369 @ v3.7.4`). `:` IS
/// included, to support a `debug: message` prefix (lvl17, e5). This is what
/// stops `info` inside `information` from matching (lvl10) and `error`
/// inside `xerror` (e6).
fn is_right_boundary(s: &[u8], pos: Option<usize>) -> bool {
    let Some(pos) = pos else {
        return true;
    };
    let Some(c) = s.get(pos) else {
        return true;
    };
    matches!(
        c,
        b' ' | b'\t'
            | b'\n'
            | b'['
            | b']'
            | b'('
            | b')'
            | b'{'
            | b'}'
            | b':'
            | b','
            | b'!'
            | b'"'
            | b'='
    )
}

/// Go's `strings.ToLower`: one rune in, one rune out, using the SIMPLE case
/// mapping.
///
/// `str::to_lowercase` is the wrong primitive here and the difference is
/// user-visible. Rust applies FULL Unicode lowercasing, under which
/// `U+0130` (`İ`) expands to two characters (`i` + `U+0307`); the line
/// `İNFO started` then lowercases to a string in which `info` does not occur
/// at all and the answer becomes `unknown`. Measured against the pinned
/// reference build, that line answers `info` (case u1, with u4 and u5
/// beside it).
///
/// Taking the first character of `char::to_lowercase()` is the simple
/// mapping: `U+0130` is the only character whose unconditional lowercase
/// expansion is longer than one character, and the first character of that
/// expansion is its simple mapping.
fn lower_simple(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    for c in line.chars() {
        out.extend(lower_first_char(c));
    }
    out
}

/// The per-character half of [`lower_simple`], kept separate so the rule
/// that must not be "tidied" back into `to_lowercase` has a name.
fn lower_first_char(c: char) -> Option<char> {
    c.to_lowercase().next()
}
