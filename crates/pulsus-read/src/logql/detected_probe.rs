//! Detected-fields probing and the tail cursor — the row-observing side
//! of `/detected_fields` and `/tail`.
//!
//! [`DetectedFieldsProbe`] and [`DetectedRowFeeder`] observe rows as they
//! stream, bounded by the feeder's scratch budget;
//! [`DetectedPagedState`] carries the paging state and
//! [`TailCursorTracker`] the tail cursor. [`FanOutGroup`] and
//! [`LabelScratch`] are the reused buffers that keep the observation
//! allocation-free per row.

use super::detected::{self, DetectedFieldOut, FieldAccumulator};
use super::error::{ReadError, TooBroadReason};
use super::rows::{SampleRow, StreamMetaRow, TailSampleRow};
use futures::Stream;
use futures::StreamExt;
use std::borrow::Cow;
use std::collections::HashMap;

use super::charge::{StreamsResultBudget, alloc_block_bytes, entry_category_bytes};
use super::exec::{EntryCategories, StreamResult, TailCursor};
use super::labels::{
    EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx, fnv1a64,
    merge_labels_with_structured_metadata, parse_flat_labels, parse_flat_labels_into,
    render_labels_json_sorted,
};
use super::pipeline::{JsonPaths, LabelCategory};

/// One fan-out group's accumulator — deliberately WITHOUT `labels_json`:
/// the map key is the single owned copy of the rendered label set, moved
/// into [`StreamResult`] when the map drains (review round 3: no
/// per-new-group key clone, which under high-cardinality fan-out is
/// effectively per-row).
#[derive(Debug)]
pub(in crate::logql) struct FanOutGroup {
    pub(in crate::logql) fingerprint: u64,
    pub(in crate::logql) service: String,
    pub(in crate::logql) entries: Vec<(i64, String)>,
    /// Parallel to [`Self::entries`] in categorised mode (issue #463),
    /// empty otherwise — the same all-or-nothing invariant
    /// [`super::exec::StreamResult::categories`] carries.
    pub(in crate::logql) categories: Vec<EntryCategories>,
}

/// Inserts one surviving fan-out entry (its `sorted_scratch` label set already
/// sorted) into the label-set-keyed group map — shared by the label-mutating
/// pipeline path and the structured-metadata merge path (issue #97), which both
/// group by the final rendered label set. The rendered `labels_json` is the map
/// key (one owned copy, moved into [`StreamResult`] at drain — no per-new-group
/// key clone); the group's `fingerprint` is a deterministic content hash of it;
/// `service` is the merged set's `service_name` or `fallback_service`.
///
/// **Charged before it retains (issue #312).** The entry is charged
/// before `line.into_owned()` — so on the `Cow::Borrowed` path the
/// refusing charge precedes the only allocation it pays for, and on the
/// `Cow::Owned` path `into_owned()` is a move — and a new group is
/// charged before the vacant-arm `insert` and before `service` is
/// cloned. ONE transient is deliberately NOT charged and is called out:
/// the map key is rendered before this function can know whether the
/// group is new. That `String` is exactly sized by
/// `render_labels_json_sorted`'s estimate, is one label set, is
/// pre-existing, and is dropped on the occupied arm; its RETAINED form
/// is charged with `grown_alloc_bytes` because that `with_capacity`
/// estimate can under-reserve when `push_json_string` escapes.
pub(in crate::logql) fn push_fanout_entry(
    label_groups: &mut HashMap<String, FanOutGroup>,
    budget: &mut StreamsResultBudget,
    sorted_scratch: &[(Cow<'_, str>, Cow<'_, str>)],
    timestamp_ns: i64,
    line: Cow<'_, str>,
    fallback_service: &str,
    categories: Option<EntryCategories>,
) -> Result<(), ReadError> {
    let labels_json = render_labels_json_sorted(sorted_scratch);
    let service: &str = sorted_scratch
        .iter()
        .find(|(k, _)| k == "service_name")
        .map(|(_, v)| v.as_ref())
        .unwrap_or(fallback_service);
    let fingerprint = fnv1a64(labels_json.as_bytes());
    push_group_entry(
        label_groups,
        budget,
        labels_json,
        fingerprint,
        service,
        timestamp_ns,
        line,
        categories,
    )
}

/// [`push_fanout_entry`]'s core, for a caller that ALREADY holds the
/// rendered group key — the issue #463 categorised fast path, whose
/// stream-category label set is the hydrated `labels_json` verbatim
/// unless a metadata value took over a stream slot, so re-rendering it
/// per row would be pure waste.
///
/// The charge order is the one [`push_fanout_entry`] documents: the group
/// charge precedes the `service` clone it pays for, and the entry charge
/// precedes `line.into_owned()`. `categories`' bytes are charged with the
/// entry (issue #463) — a third element the budget did not price is
/// exactly how the retention cap stops bounding the result.
#[allow(clippy::too_many_arguments)]
pub(in crate::logql) fn push_group_entry(
    label_groups: &mut HashMap<String, FanOutGroup>,
    budget: &mut StreamsResultBudget,
    labels_json: String,
    fingerprint: u64,
    service: &str,
    timestamp_ns: i64,
    line: Cow<'_, str>,
    categories: Option<EntryCategories>,
) -> Result<(), ReadError> {
    let cat_bytes = categories.as_ref().map_or(0, entry_category_bytes);
    match label_groups.entry(labels_json) {
        std::collections::hash_map::Entry::Occupied(e) => {
            budget.charge_entry(line.len(), cat_bytes)?;
            let g = e.into_mut();
            g.entries.push((timestamp_ns, line.into_owned()));
            if let Some(c) = categories {
                g.categories.push(c);
            }
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            budget.charge_group(e.key(), service)?;
            budget.charge_entry(line.len(), cat_bytes)?;
            e.insert(FanOutGroup {
                fingerprint,
                service: service.to_string(),
                entries: vec![(timestamp_ns, line.into_owned())],
                categories: categories.into_iter().collect(),
            });
        }
    }
    Ok(())
}

/// Splits one row's final label vector into its stream half (left in
/// `scratch`, in place) and its two non-stream halves (returned), using
/// the per-label categories the pipeline reported (issue #463).
///
/// **`cats` is parallel to `scratch` in the pipeline's own order**, which
/// is why this runs BEFORE the caller sorts: sorting first would
/// desynchronise the two vectors. `retain` visits in order, so walking a
/// cursor over `cats` alongside it is exact.
///
/// The returned vectors are then sorted by key, which is the order
/// `pkg/util/marshal/query.go:404-470 @ grafana/loki v3.7.4 b318f282`
/// renders them in (a Go `map[string]string` marshalled by
/// `encoding/json`, which sorts its keys).
///
/// Allocates only for the labels that actually leave the stream category:
/// an entry whose whole label set is stream-category returns two empty
/// `Vec`s and copies nothing.
pub(in crate::logql) fn split_categories(
    scratch: &mut LabelScratch<'_>,
    cats: &[LabelCategory],
) -> EntryCategories {
    let mut out = EntryCategories::default();
    for (i, (k, v)) in scratch.iter().enumerate() {
        match cats.get(i) {
            Some(LabelCategory::StructuredMetadata) => {
                out.structured_metadata.push((k.to_string(), v.to_string()))
            }
            Some(LabelCategory::Parsed) => out.parsed.push((k.to_string(), v.to_string())),
            // A missing category cannot happen — `run_mode_into` fills one
            // per label — and treating it as stream is the shape that
            // downgrades rather than mis-files.
            Some(LabelCategory::Stream) | None => {}
        }
    }
    let mut i = 0usize;
    scratch.retain(|_| {
        let keep = !matches!(
            cats.get(i),
            Some(LabelCategory::StructuredMetadata) | Some(LabelCategory::Parsed)
        );
        i += 1;
        keep
    });
    out.structured_metadata.sort_unstable();
    out.parsed.sort_unstable();
    out
}

/// A reusable label scratch whose `Cow` entries borrow from the row's merged
/// base labels (lifetime `'a`) or own rewritten values — the buffer
/// `run_into` fills for structured-metadata-bearing rows (issue #97).
pub(in crate::logql) type LabelScratch<'a> = Vec<(Cow<'a, str>, Cow<'a, str>)>;

/// Runs one structured-metadata-bearing row through the pipeline over `merged`
/// (base + SM labels) and fans its surviving line into `label_groups`, reusing
/// `scratch`'s heap allocation across rows. `scratch` is taken BY VALUE and
/// returned (cleared) rather than borrowed `&mut`, because `run_into`'s output
/// labels borrow `merged` — whose contents are rewritten every row — so the
/// Cow scratch needs a FRESH lifetime per call; a hoisted `&mut Vec<Cow<'a>>`
/// binding cannot provide that (the merge buffer's `.clear()` would conflict
/// with an outstanding borrow). Passing by value gives each call its own
/// lifetime while [`recycle_label_scratch`] hands the same allocation back for
/// the next row (issue #97 review round 1, finding 2 / AC-12).
#[allow(clippy::too_many_arguments)]
pub(in crate::logql) fn eval_structured_metadata_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    merged: &'a [(String, String)],
    sm: &'a StructuredMetadataCtx,
    label_groups: &mut HashMap<String, FanOutGroup>,
    budget: &mut StreamsResultBudget,
    timestamp_ns: i64,
    service: &str,
    mut scratch: LabelScratch<'a>,
    mut cat_scratch: Option<&mut Vec<LabelCategory>>,
) -> (bool, Result<LabelScratch<'a>, ReadError>) {
    let run = match cat_scratch.as_deref_mut() {
        None => compiled.run_into_with_sm(body, merged, timestamp_ns, sm, &mut scratch),
        Some(cats) => compiled.run_into_with_sm_categorized(
            body,
            merged,
            timestamp_ns,
            sm,
            &mut scratch,
            cats,
        ),
    };
    let survived = match run {
        Ok(Some(line)) => {
            // Issue #463: the split runs BEFORE the sort, because the
            // pipeline's category vector is parallel to the label vector
            // in the order the pipeline left it.
            let categories = cat_scratch
                .as_deref()
                .map(|cats| split_categories(&mut scratch, cats));
            scratch.sort_unstable();
            // The result-budget breach (issue #312) is the same bounded
            // 422 class; clear the scratch first so the recycling
            // contract holds on the way out.
            if let Err(e) = push_fanout_entry(
                label_groups,
                budget,
                &scratch,
                timestamp_ns,
                line,
                service,
                categories,
            ) {
                scratch.clear();
                return (false, Err(e));
            }
            true
        }
        Ok(None) => false,
        // Template render-budget breach: the whole query fails (bounded
        // 422 — issue #230 follow-up).
        Err(e) => return (false, Err(e.into())),
    };
    // Drop every borrow of `merged` before the buffer is recycled for reuse.
    scratch.clear();
    (survived, Ok(scratch))
}

/// Re-tags a cleared borrowed-label scratch's (now empty) heap allocation as
/// `'static` so it can be reused by the next SM row, whose `merged` base labels
/// live for only one iteration. Safe: the vector is emptied first, so no borrow
/// survives the re-tag; the allocation is preserved by the in-place
/// `into_iter().map().collect()` (identical element layout). If that reuse ever
/// regressed it would only reallocate — never misbehave — and AC-12 gates the
/// reuse from outside the crate.
pub(in crate::logql) fn recycle_label_scratch(
    mut scratch: LabelScratch<'_>,
) -> LabelScratch<'static> {
    scratch.clear();
    scratch
        .into_iter()
        .map(|(k, v)| (Cow::Owned(k.into_owned()), Cow::Owned(v.into_owned())))
        .collect()
}

/// The slot cap `DetectedRowFeeder::trim` applies to each carried `Vec`
/// scratch, and the byte cap it applies to each carried `String` slot
/// (issue #244): a row wider than this still processes fully — the caps
/// bound only what the feeder CARRIES to the next row.
const MAX_FEEDER_SCRATCH_SLOTS: usize = 4096;
const MAX_FEEDER_SCRATCH_STRING_BYTES: usize = 4096;

/// DERIVED from `size_of`, never calibrated: the five-term bound on the
/// heap [`DetectedRowFeeder`]'s buffers can carry between rows after
/// `trim()` — `merge_buf` + `sm_buf` (each `MAX_FEEDER_SCRATCH_SLOTS`
/// slots of `(String, String)`), `label_scratch`
/// (`MAX_FEEDER_SCRATCH_SLOTS` slots of `(Cow, Cow)`), and the two
/// `sm_ctx` `String`s (`MAX_FEEDER_SCRATCH_STRING_BYTES` each), every
/// term rounded through [`alloc_block_bytes`]. Content carried by the
/// `String`s inside a trimmed `Vec`'s slots is zero — `trim` clears
/// before capping, so a kept spine is empty.
const fn feeder_scratch_bytes() -> u64 {
    let pair = (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(String, String)>()) as u64;
    let cow =
        (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64;
    2 * alloc_block_bytes(pair)
        + alloc_block_bytes(cow)
        + 2 * alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64)
}

/// The published bound on what the detected-fields feeder carries between
/// rows (issue #244, claim C1) — `1_196_032 B` on 64-bit targets, gated
/// by `detected_fields_witness.rs`.
pub const MAX_FEEDER_SCRATCH_BYTES: u64 = feeder_scratch_bytes();

/// The per-row scratch state both detected-fields read paths stream
/// through (issue #244) — one row is live at a time; the carried
/// capacity is bounded by [`MAX_FEEDER_SCRATCH_BYTES`] via `trim()` on
/// every exit path of `feed_row`.
///
/// Two rules (`&mut Vec<T>` is invariant in `T` — see
/// `eval_structured_metadata_row`'s rationale): **R1** every scratch
/// stored in a struct is `LabelScratch<'static>`; **R2** a scratch
/// crosses a lifetime only by MOVE (`mem::take`, by-value,
/// [`recycle_label_scratch`]).
#[derive(Debug, Default)]
pub(in crate::logql) struct DetectedRowFeeder {
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
    /// ONE scratch serves both the pipeline pass and the auto-parse pass
    /// (issue #244) — `run_into_with_sm` returns a `Cow` over
    /// `body`/`self`, never over `labels`.
    label_scratch: LabelScratch<'static>,
}

impl DetectedRowFeeder {
    /// All buffers empty; no allocation.
    pub(in crate::logql) fn new() -> Self {
        Self::default()
    }

    /// Clears every reusable buffer and DROPS any whose capacity exceeds
    /// the cap. Called on EVERY exit path of `feed_row` — one return
    /// point, after it. The ONLY place capacity is released.
    fn trim(&mut self) {
        fn trim_vec<T>(v: &mut Vec<T>) {
            v.clear();
            if v.capacity() > MAX_FEEDER_SCRATCH_SLOTS {
                *v = Vec::new();
            }
        }
        fn trim_str(s: &mut String) {
            s.clear();
            if s.capacity() > MAX_FEEDER_SCRATCH_STRING_BYTES {
                *s = String::new();
            }
        }
        trim_vec(&mut self.merge_buf);
        trim_vec(&mut self.sm_buf);
        trim_vec(&mut self.label_scratch);
        trim_str(&mut self.sm_ctx.err);
        trim_str(&mut self.sm_ctx.details);
        self.sm_ctx.has_ordinary = false;
    }

    /// The [`alloc_block_bytes`]-rounded heap the five carried buffers
    /// hold right now — the quantity [`MAX_FEEDER_SCRATCH_BYTES`] bounds
    /// after every `feed_row`.
    fn scratch_capacity_bytes(&self) -> u64 {
        alloc_block_bytes((self.merge_buf.capacity() * size_of::<(String, String)>()) as u64)
            .saturating_add(alloc_block_bytes(
                (self.sm_buf.capacity() * size_of::<(String, String)>()) as u64,
            ))
            .saturating_add(alloc_block_bytes(
                (self.label_scratch.capacity()
                    * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64,
            ))
            .saturating_add(alloc_block_bytes(self.sm_ctx.err.capacity() as u64))
            .saturating_add(alloc_block_bytes(self.sm_ctx.details.capacity() as u64))
    }

    /// Streams ONE sampled row through the pipeline into `acc` and drops
    /// it (issue #244): merge SM labels if present, run the pipeline, and
    /// on survival observe SM pairs (re-parsed into the merge-drained
    /// `sm_buf`), pipeline-extracted pairs, then the auto-parse pass.
    /// `Ok(true)` = the row survived the pipeline (counts toward
    /// `line_limit`); a fingerprint that never hydrated is `Ok(false)`.
    /// An `Err` is the #230 template render-budget breach — the whole
    /// query fails, exactly as before.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::logql) fn feed_row(
        &mut self,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
    ) -> Result<bool, ReadError> {
        let result: Result<bool, ReadError> = 'row: {
            let Some(base) = base_labels.get(&fingerprint) else {
                break 'row Ok(false);
            };
            let Self {
                merge_buf,
                sm_buf,
                sm_ctx,
                label_scratch,
            } = self;
            let has_sm = !structured_metadata.is_empty();
            if has_sm {
                // Drains `sm_buf` into `merge_buf` — leaving `sm_buf`
                // free for the post-survival SM re-parse below.
                merge_labels_with_structured_metadata(
                    base,
                    structured_metadata,
                    merge_buf,
                    sm_buf,
                    sm_ctx,
                );
            }
            let run_base: &[(String, String)] = if has_sm { &*merge_buf } else { base };
            let sm: &StructuredMetadataCtx = if has_sm {
                &*sm_ctx
            } else {
                &EMPTY_STRUCTURED_METADATA
            };
            let sm_reparse: Option<(&str, &mut Vec<(String, String)>)> = if has_sm {
                Some((structured_metadata, sm_buf))
            } else {
                None
            };
            let scratch = std::mem::take(label_scratch); // MOVE OUT (R2)
            let (survived, used) = observe_detected_row(
                compiled,
                body,
                run_base,
                timestamp_ns,
                sm,
                sm_reparse,
                acc,
                scratch,
            );
            match used {
                Ok(returned) => {
                    *label_scratch = returned;
                    Ok(survived)
                }
                Err(e) => break 'row Err(e),
            }
        };
        self.trim();
        result
    }
}

/// Runs one sampled row through the query pipeline and, when it
/// survives, feeds the [`FieldAccumulator`] (issue #170): structured-
/// metadata pairs first (no parser attribution), then pipeline-extracted
/// keys not present in the merged base (no parser attribution;
/// `__error__`/`__error_details__` excluded inside `observe_pair`), then
/// json-first/logfmt-fallback auto-detection on the POST-pipeline line.
/// `sm: &'a StructuredMetadataCtx` is the #238 signature, preserved.
///
/// Two changes vs the pre-#244 shape; their effect on per-row allocation
/// is measured by AC 13 of the #244 plan and stated only there — see
/// **C2 (Q1+Q2)**, plan §A:
///  * ONE scratch serves both the pipeline pass and the auto-parse pass —
///    `run_into_with_sm` returns a `Cow<'a, str>` over `body`/`self`, NOT
///    over `labels`;
///  * the SM pairs are RE-PARSED into the (merge-drained) `sm_buf`
///    instead of a third owned buffer. Observation ORDER unchanged — SM
///    pairs, then pipeline pairs, then auto-parse — and the re-parse runs
///    only when the row SURVIVED
///    (`detected_fields_matched_count_is_post_pipeline_dropped_rows_do_not_count`).
///    Parse count per SM row: 2 before → 1 + 1-if-surviving.
///
/// `scratch` is taken by value and returned for recycling — same
/// per-row-lifetime rationale as [`eval_structured_metadata_row`].
#[allow(clippy::too_many_arguments)]
fn observe_detected_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    run_base: &'a [(String, String)],
    ts_ns: i64,
    sm: &'a StructuredMetadataCtx,
    sm_reparse: Option<(&str, &mut Vec<(String, String)>)>,
    acc: &mut FieldAccumulator,
    scratch: LabelScratch<'static>,
) -> (bool, Result<LabelScratch<'static>, ReadError>) {
    let mut scratch: LabelScratch<'a> = scratch; // 'static -> 'a by covariance
    let line = match compiled.run_into_with_sm(body, run_base, ts_ns, sm, &mut scratch) {
        Ok(Some(line)) => line,
        Ok(None) => {
            scratch.clear();
            return (false, Ok(recycle_label_scratch(scratch)));
        }
        // Template render-budget breach fails the sampling query too —
        // the bounded 422 (issue #230 follow-up).
        Err(e) => return (false, Err(e.into())),
    };
    if let Some((sm_json, buf)) = sm_reparse {
        // D1 (explanatory, #244 plan §6): the SM re-parse, on survival
        // only.
        buf.clear();
        parse_flat_labels_into(sm_json, buf);
        for (k, v) in buf.iter() {
            acc.observe_pair(k, v, detected::FieldSource::Unattributed);
        }
    }
    for (k, v) in scratch.iter() {
        if run_base.iter().any(|(bk, _)| bk.as_str() == k.as_ref()) {
            continue;
        }
        acc.observe_pair(k.as_ref(), v.as_ref(), detected::FieldSource::Unattributed);
    }
    // Drop every borrow of `run_base` before the buffer is recycled.
    scratch.clear();
    let scratch: LabelScratch<'static> = recycle_label_scratch(scratch);
    // D2 (explanatory, #244 plan §6): the auto-parse pass.
    (true, auto_parse_observe(line.as_ref(), acc, scratch))
}

/// The auto-parse pass over the post-pipeline line, reusing the SAME
/// (recycled) scratch the pipeline pass used (issue #244).
///
/// This pass runs the bare `| json` flatten on EVERY sampled line, so it
/// is where `/detected_fields` pays the expansion unconditionally
/// (issue #287); a key-budget breach fails the sampling query with the
/// bounded 422, exactly as the user-pipeline pass above does. The
/// scratch is cleared on BOTH paths before it crosses back to
/// `'static` (rule R2) — on the breach path it is then dropped, and
/// `feed_row`'s `trim()` (which runs on every exit) is what accounts
/// for the capacity.
///
/// The issue #254 json-path sink is a ROW-LOCAL (deliberately NOT a
/// sixth carried feeder buffer, which would move
/// `MAX_FEEDER_SCRATCH_BYTES`): a row that parses as neither json nor
/// logfmt leaves it at `Vec::new()`, so it allocates nothing at all, and
/// a json row pays one spine plus one `Vec<String>` per captured leaf —
/// the same per-leaf slice the reference allocates at
/// `pkg/logql/log/parser.go:162,196 @ v3.7.4`. It never reaches the
/// query path, whose json parser has capture off.
fn auto_parse_observe<'l>(
    line: &'l str,
    acc: &mut FieldAccumulator,
    scratch: LabelScratch<'l>,
) -> Result<LabelScratch<'static>, ReadError> {
    let mut scratch = scratch;
    let mut paths = JsonPaths::default();
    let parsed = detected::auto_parse_into(line, &mut scratch, &mut paths);
    if let Ok(Some(parser)) = parsed {
        for (i, (k, v)) in scratch.iter().enumerate() {
            let source = if parser == "json" {
                detected::FieldSource::Json { path: paths.get(i) }
            } else {
                detected::FieldSource::Logfmt
            };
            acc.observe_pair(k.as_ref(), v.as_ref(), source);
        }
    }
    scratch.clear();
    let scratch = recycle_label_scratch(scratch);
    match parsed {
        Ok(_) => Ok(scratch),
        Err(e) => Err(e.into()),
    }
}

/// AC 13's baseline (issue #244): a TEST-ONLY TRANSCRIPTION of the
/// pre-#244 per-row shape — the owned `added` vector (plan-pinned
/// `d145ded` `exec.rs:7159-7163`, whole function `:7148-7173`; identical
/// at merge base `a627a6c` modulo the #230 `ts_ns`/`Result` threading
/// transcribed here from the `a627a6c` text) and
/// [`detected::auto_parse_legacy_shape`]'s owned return
/// (`detected.rs:241-254`) — run over the SAME [`DetectedRowFeeder`]
/// state, so the measured difference is the shape change and nothing
/// else.
///
/// SCOPE — **C2 (Q1+Q2)**, #244 plan §A, both qualifiers: Q1, only the
/// four row shapes AC 13 measures; Q2, a comparison between two helpers
/// in THIS tree, not a whole-program before/after against the shipped old
/// binary. AC 13e enforces that these helpers never reach production; it
/// does NOT enforce that they still match `d145ded`. Their fidelity is
/// human-verified against the cited line ranges at implementation time
/// (AC 13f) and recorded in the implementation notes; #244 plan §6.3
/// states what would make it mechanical.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
fn observe_detected_row_legacy_shape<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    run_base: &'a [(String, String)],
    ts_ns: i64,
    sm: &'a StructuredMetadataCtx,
    sm_pairs: &[(String, String)],
    acc: &mut FieldAccumulator,
    mut scratch: LabelScratch<'a>,
) -> (bool, Result<LabelScratch<'a>, ReadError>) {
    let survived = match compiled.run_into_with_sm(body, run_base, ts_ns, sm, &mut scratch) {
        Ok(Some(line)) => {
            acc.observe_structured_metadata(sm_pairs);
            let added: Vec<(String, String)> = scratch
                .iter()
                .filter(|(k, _)| !run_base.iter().any(|(bk, _)| bk.as_str() == k.as_ref()))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            acc.observe_parsed(&added, detected::FieldSource::Unattributed);
            if let Some((parser, pairs)) = detected::auto_parse_legacy_shape(line.as_ref()) {
                // The pre-#244 shape predates #254's path capture, so the
                // json arm attributes with NO path — this helper measures
                // allocation shape and never runs in production (AC 13e).
                let source = if parser == "json" {
                    detected::FieldSource::Json { path: None }
                } else {
                    detected::FieldSource::Logfmt
                };
                acc.observe_parsed(&pairs, source);
            }
            true
        }
        Ok(None) => false,
        Err(e) => return (false, Err(e.into())),
    };
    scratch.clear();
    (survived, Ok(scratch))
}

/// Incremental twin of [`advance_tail_cursor`] for the streaming paged
/// loop (issue #244): observes every drained row — including rows past
/// `line_limit` that are not fed; the cursor advances over the RAW page —
/// and folds into the previous cursor at page end. Equivalence over
/// randomized sequences is pinned by AC 17's tests below.
///
/// `pub(in crate::logql)` since issue #312: the streams paged loop and
/// `tail_poll` stream their pages too, so the SAME incremental cursor
/// serves all three — `advance_tail_cursor` has no production caller
/// left and survives only as this tracker's independent oracle.
#[derive(Debug)]
pub(in crate::logql) struct TailCursorTracker {
    tuple: Option<(i64, u64, u64)>,
    run: u32,
    rows: u32,
}

impl TailCursorTracker {
    pub(in crate::logql) fn new() -> Self {
        Self {
            tuple: None,
            run: 0,
            rows: 0,
        }
    }

    /// Called for EVERY drained row.
    pub(in crate::logql) fn observe(
        &mut self,
        timestamp_ns: i64,
        fingerprint: u64,
        body_hash: u64,
    ) {
        self.rows = self.rows.saturating_add(1);
        let bt = (timestamp_ns, fingerprint, body_hash);
        match self.tuple {
            Some(t) if t == bt => self.run = self.run.saturating_add(1),
            _ => {
                self.tuple = Some(bt);
                self.run = 1;
            }
        }
    }

    /// `(next cursor, rows drained)` — an empty page keeps `prev`; a
    /// page ending on `prev`'s tuple carries its `seen` (the `OFFSET`
    /// already skipped those), exactly [`advance_tail_cursor`].
    pub(in crate::logql) fn finish(self, prev: Option<TailCursor>) -> (Option<TailCursor>, u32) {
        match self.tuple {
            None => (prev, self.rows),
            Some(bt) => {
                let carry = match prev {
                    Some(c) if c.tuple == bt => c.seen,
                    _ => 0,
                };
                (
                    Some(TailCursor {
                        tuple: bt,
                        seen: self.run.saturating_add(carry),
                    }),
                    self.rows,
                )
            }
        }
    }
}

/// The #90 branch split as a pure truth table (issue #244, AC 16c): a
/// `ScanBudgetBytes` overflow AFTER at least one drained page keeps the
/// accumulated prefix (`Ok(true)` = terminate-PARTIAL); on the FIRST page
/// (`spent == 0`) it is a genuinely too-broad query and propagates —
/// HTTP 422, regardless of how many rows were already delivered (the
/// first-page rule; streaming must NOT turn that 422 into a 200). Every
/// other error propagates.
pub(in crate::logql) fn classify_page_error(
    mapped: ReadError,
    spent: u64,
) -> Result<bool, ReadError> {
    if spent > 0
        && matches!(
            mapped,
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
        )
    {
        return Ok(true);
    }
    Err(mapped)
}

/// The streaming paged-loop state (issue #244): everything
/// `run_detected_fields_paged` carries between pages, with the one-page
/// drain factored into [`DetectedPagedState::absorb_page`] so the whole
/// loop body is hermetically testable over injected streams.
#[derive(Debug)]
pub(in crate::logql) struct DetectedPagedState {
    pub(in crate::logql) feeder: DetectedRowFeeder,
    pub(in crate::logql) cursor: Option<TailCursor>,
    pub(in crate::logql) spent: u64,
    pub(in crate::logql) matched: u32,
    pub(in crate::logql) page_size: u32,
    pub(in crate::logql) line_limit: u32,
    pub(in crate::logql) budget: u64,
}

impl DetectedPagedState {
    /// Drains ONE already-opened page to completion, streaming each row
    /// through the feeder as it arrives, then returns the loop's
    /// decision: `Ok(None)` continue / `Ok(Some(false))`
    /// terminate-COMPLETE / `Ok(Some(true))` terminate-PARTIAL / `Err`
    /// propagate. The drain stops at the FIRST error — exactly what the
    /// pre-#244 per-row `?` did. `line_limit` stops FEEDING, never
    /// DRAINING: `read_bytes` is meaningful only after a full drain
    /// (`wait_end_of_query = 1`) and `fetched < page_size` is the
    /// window-exhaustion terminal. Generic over the stream AND its error
    /// so the public seam never names `ChError`.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::logql) async fn absorb_page<S, E>(
        &mut self,
        stream: &mut S,
        read_bytes: impl FnOnce(&S) -> Option<u64>,
        map_err: impl Fn(E) -> ReadError,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, E>> + Unpin,
    {
        let mut tracker = TailCursorTracker::new();
        let mut page_err: Option<ReadError> = None;
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(e) => {
                    page_err = Some(map_err(e));
                    break;
                }
            };
            // Advance over the RAW page, not survivors — a page entirely
            // dropped by the pipeline must never stall the walk.
            tracker.observe(row.timestamp_ns, row.fingerprint, row.body_hash);
            if self.matched >= self.line_limit {
                continue;
            }
            match self.feeder.feed_row(
                row.fingerprint,
                row.timestamp_ns,
                &row.body,
                &row.structured_metadata,
                base_labels,
                compiled,
                acc,
            ) {
                Ok(true) => self.matched += 1,
                Ok(false) => {}
                Err(e) => {
                    page_err = Some(e);
                    break;
                }
            }
        }
        let (cursor, fetched) = tracker.finish(self.cursor.take());
        self.cursor = cursor;
        if let Some(mapped) = page_err {
            // Mid-page prefix retention (issue #244, pinned by AC 16b):
            // rows delivered before the error are already accumulated;
            // the prefix boundary is NOT required to align with a page
            // boundary.
            return classify_page_error(mapped, self.spent).map(Some);
        }
        let read = read_bytes(stream).unwrap_or_else(|| self.budget.saturating_sub(self.spent));
        self.spent = self.spent.saturating_add(read);
        if self.matched >= self.line_limit {
            // Post-pipeline limit filled — complete, never partial.
            return Ok(Some(false));
        }
        if fetched < self.page_size {
            // Window exhausted — complete over the whole window (this is
            // the branch that finds late-occurring matches).
            return Ok(Some(false));
        }
        Ok(None)
    }
}

/// The public hermetic test seam for the #244 streaming detected-fields
/// machinery — accumulator + feeder + paged state, all private inside.
/// `#[doc(hidden)]`: consumed only by `tests/detected_fields_witness.rs`
/// and the `logqltest` corpus runner, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct DetectedFieldsProbe {
    acc: FieldAccumulator,
    state: DetectedPagedState,
    base_labels: HashMap<u64, Vec<(String, String)>>,
    /// The legacy shape's third owned SM-observation buffer — pre-#244
    /// `feed_detected_rows` carried one across a page's rows; held here
    /// (test-only) so `feed_row_legacy_shape` reproduces that steady
    /// state without adding a buffer to the production feeder.
    legacy_sm_obs: Vec<(String, String)>,
}

#[doc(hidden)]
impl DetectedFieldsProbe {
    pub fn new(line_limit: u32, field_limit: u32) -> Self {
        Self::with_byte_budget(line_limit, field_limit, detected::MAX_DETECTED_FIELD_BYTES)
    }

    pub fn with_byte_budget(line_limit: u32, field_limit: u32, retention_budget: u64) -> Self {
        Self {
            acc: FieldAccumulator::with_byte_budget(field_limit, retention_budget),
            state: DetectedPagedState {
                feeder: DetectedRowFeeder::new(),
                cursor: None,
                spent: 0,
                matched: 0,
                page_size: line_limit.max(1),
                line_limit,
                budget: u64::MAX,
            },
            base_labels: HashMap::new(),
            legacy_sm_obs: Vec::new(),
        }
    }

    pub fn add_stream(&mut self, fingerprint: u64, labels: &[(String, String)]) {
        self.base_labels.insert(fingerprint, labels.to_vec());
    }

    /// `parser` is `None` (unattributed), `Some("json")` or
    /// `Some("logfmt")` — the seam predates issue #254's json paths and
    /// carries none, so a `Some("json")` observation here attributes the
    /// parser without a path. The corpus runner and the live suites reach
    /// paths through the production [`DetectedFieldsProbe::feed_row`].
    pub fn observe_pair(&mut self, key: &str, value: &str, parser: Option<&'static str>) {
        let source = match parser {
            None => detected::FieldSource::Unattributed,
            Some("json") => detected::FieldSource::Json { path: None },
            Some("logfmt") => detected::FieldSource::Logfmt,
            Some(other) => panic!("unknown parser name {other:?} (json | logfmt)"),
        };
        self.acc.observe_pair(key, value, source);
    }

    /// One row through the PRODUCTION feeder; applies the
    /// `matched >= line_limit` feeding gate.
    pub fn feed_row(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
    ) -> Result<bool, ReadError> {
        if self.state.matched >= self.state.line_limit {
            return Ok(false);
        }
        let survived = self.state.feeder.feed_row(
            fingerprint,
            timestamp_ns,
            body,
            structured_metadata,
            &self.base_labels,
            compiled,
            &mut self.acc,
        )?;
        if survived {
            self.state.matched += 1;
        }
        Ok(survived)
    }

    /// AC 13's baseline: the SAME row and the SAME feeder state through
    /// the pre-#244 per-row OBSERVE shape (the owned `sm_obs` parse, the
    /// owned `added` vector, [`detected::auto_parse_legacy_shape`]'s
    /// owned return — the transcribed `d145ded` ranges). The wrapper
    /// shares the feeder's carry policy (`trim()` on exit) with the new
    /// path deliberately: AC 13's contract is that "the measured
    /// difference is exactly the shape change", and the transcription's
    /// cited ranges are the observe-level helpers — a baseline that also
    /// reverted the #244 carry policy would fold trim's capacity-drop
    /// cost into a comparison that is meant to isolate the per-row
    /// owned-copy shape. Never a production path (AC 13e).
    pub fn feed_row_legacy_shape(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
    ) -> Result<bool, ReadError> {
        if self.state.matched >= self.state.line_limit {
            return Ok(false);
        }
        let Some(base) = self.base_labels.get(&fingerprint) else {
            return Ok(false);
        };
        let feeder = &mut self.state.feeder;
        let has_sm = !structured_metadata.is_empty();
        if has_sm {
            self.legacy_sm_obs.clear();
            parse_flat_labels_into(structured_metadata, &mut self.legacy_sm_obs);
            merge_labels_with_structured_metadata(
                base,
                structured_metadata,
                &mut feeder.merge_buf,
                &mut feeder.sm_buf,
                &mut feeder.sm_ctx,
            );
        }
        let run_base: &[(String, String)] = if has_sm { &feeder.merge_buf } else { base };
        let sm_pairs: &[(String, String)] = if has_sm { &self.legacy_sm_obs } else { &[] };
        let sm: &StructuredMetadataCtx = if has_sm {
            &feeder.sm_ctx
        } else {
            &EMPTY_STRUCTURED_METADATA
        };
        let scratch = std::mem::take(&mut feeder.label_scratch);
        let (survived, used) = observe_detected_row_legacy_shape(
            compiled,
            body,
            run_base,
            timestamp_ns,
            sm,
            sm_pairs,
            &mut self.acc,
            scratch,
        );
        let result = match used {
            Ok(returned) => {
                feeder.label_scratch = recycle_label_scratch(returned);
                Ok(survived)
            }
            Err(e) => Err(e),
        };
        // The shared carry policy (see the doc above): the baseline trims
        // exactly like the new path, so the measured windows differ only
        // in the observe-level shape. (`legacy_sm_obs` is legacy-only
        // state — its untrimmed reuse IS part of the old shape.)
        self.state.feeder.trim();
        let survived = result?;
        if survived {
            self.state.matched += 1;
        }
        Ok(survived)
    }

    /// One injected page through the REAL paged-loop body.
    pub async fn absorb_page<S>(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        stream: &mut S,
        read_bytes: u64,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, ReadError>> + Unpin,
    {
        self.state
            .absorb_page(
                stream,
                |_| Some(read_bytes),
                |e| e,
                &self.base_labels,
                compiled,
                &mut self.acc,
            )
            .await
    }

    pub fn matched(&self) -> u32 {
        self.state.matched
    }

    pub fn charged(&self) -> u64 {
        self.acc.charged()
    }

    pub fn peak_charged(&self) -> u64 {
        self.acc.peak_charged()
    }

    pub fn scratch_capacity_bytes(&self) -> u64 {
        self.state.feeder.scratch_capacity_bytes()
    }

    pub fn finish(self) -> (Vec<DetectedFieldOut>, bool) {
        self.acc.finish()
    }
}

/// Fan-out for structured-metadata-bearing rows on the line-filter-only fast
/// path (issue #97), accumulated PER ROW instead of per page (issue #312):
/// the fast path no longer stages an `sm_rows` vector, so a row's body is
/// live once — charged — rather than twice.
///
/// All filtering is already applied in SQL and no pipeline runs, so each SM
/// row's response label set is its stream's base labels merged with its
/// parsed structured metadata; each distinct merged set is its own stream
/// (Loki's per-entry structured-metadata fan-out — see the #97 oracle
/// probe). Grouping/fingerprinting matches the [`StreamAccumulator`] SM
/// branch so fast- and transform-path results are byte-consistent. **No-SM
/// rows never reach here** — they stay on the by-fingerprint fast path, so
/// its zero-per-row profile and byte-identity hold (AC-8).
///
/// [`StreamAccumulator`]: super::exec::StreamAccumulator
#[derive(Debug, Default)]
pub(in crate::logql) struct SmFanOutAccumulator {
    base_cache: HashMap<u64, Vec<(String, String)>>,
    groups: HashMap<String, FanOutGroup>,
    // Reused across rows (clear + refill, capacity-amortized) — never a fresh
    // per-row allocation of the label vector itself. `sm_buf` is the SM-pair
    // parse scratch (see `merge_labels_with_structured_metadata`).
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
}

impl SmFanOutAccumulator {
    /// Absorbs ONE structured-metadata-bearing row. A row whose
    /// fingerprint is absent from `meta` is skipped — exactly what the
    /// pre-#312 drain's `filter_map` dropped.
    pub(in crate::logql) fn push_row(
        &mut self,
        row: &SampleRow,
        meta: &HashMap<u64, StreamMetaRow>,
        budget: &mut StreamsResultBudget,
    ) -> Result<(), ReadError> {
        let Some(m) = meta.get(&row.fingerprint) else {
            return Ok(());
        };
        let base = self
            .base_cache
            .entry(row.fingerprint)
            .or_insert_with(|| parse_flat_labels(&m.labels));
        // Merge base + SM (colliding SM keys renamed `_extracted`, per the
        // oracle — no duplicate keys under any collision pattern), then sort for
        // canonical rendering. NO PIPELINE runs on this path, so the
        // reserved-SM materialisation gate is applied here, by
        // `append_visible` (issue #238): a lone `__error_details__` SM entry
        // must not surface (live-probed — the reference's clean-builder fast
        // path skips it), while an `__error__` SM entry must.
        merge_labels_with_structured_metadata(
            base,
            &row.structured_metadata,
            &mut self.merge_buf,
            &mut self.sm_buf,
            &mut self.sm_ctx,
        );
        self.sm_ctx.append_visible(&mut self.merge_buf);
        self.merge_buf.sort_unstable();
        let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = self
            .merge_buf
            .iter()
            .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
            .collect();
        // BORROWED, so the refusing charge inside `push_fanout_entry`
        // precedes the only body copy it pays for (issue #312 — this was
        // an unconditional `row.body.clone()`).
        push_fanout_entry(
            &mut self.groups,
            budget,
            &sorted,
            row.timestamp_ns,
            Cow::Borrowed(row.body.as_str()),
            &m.service,
            None,
        )
    }

    pub(in crate::logql) fn into_streams(self) -> Vec<StreamResult> {
        self.groups
            .into_iter()
            .map(|(labels_json, g)| StreamResult {
                fingerprint: g.fingerprint,
                service: g.service,
                labels_json,
                entries: g.entries,
                categories: g.categories,
            })
            .collect()
    }
}

/// The issue #463 categorised split for a NO-PIPELINE structured-metadata
/// row: the fast path runs no `CompiledPipeline`, so there is no
/// `LabelCategory` vector to read and the three categories are derived
/// from the merge itself.
///
/// - **stream** — the merged base's leading `stream_label_count` entries,
///   MINUS any slot a metadata value took over
///   ([`StructuredMetadataCtx::sm_over_stream`], the double collision);
/// - **structured metadata** — everything past `stream_label_count`
///   (the row's ordinary metadata, post-`_extracted` rename) plus those
///   taken-over stream slots;
/// - **parsed** — the visible reserved slots only. `__error__` /
///   `__error_details__` arriving AS structured metadata are routed to
///   the error slots by `LabelsBuilder.Add`
///   (`pkg/logql/log/labels.go:399-408 @ grafana/loki v3.7.4 b318f282`)
///   and then filed under `parsed` by `LabelsResult` (`:610-614`) — so on
///   this path they belong in `parsed`, never in `structuredMetadata`.
///
/// `merge_buf` is left holding the STREAM half only, sorted, so the
/// caller renders the `stream` object straight from it.
pub(in crate::logql) fn split_merged_categories(
    merge_buf: &mut Vec<(String, String)>,
    sm_ctx: &StructuredMetadataCtx,
) -> EntryCategories {
    let base_len = sm_ctx.stream_label_count.unwrap_or(merge_buf.len());
    // Stable in-place partition: stream entries to the front. The
    // predicate reads index `r` before this iteration's swap writes it,
    // and every earlier swap wrote only indices `< r`, so each pair is
    // classified at its ORIGINAL position. `swap` (not `clone`) keeps the
    // owned `String`s moving rather than copying.
    let mut w = 0usize;
    for r in 0..merge_buf.len() {
        let is_stream = r < base_len && !sm_ctx.sm_over_stream.iter().any(|s| *s == merge_buf[r].0);
        if is_stream {
            merge_buf.swap(w, r);
            w += 1;
        }
    }
    let mut out = EntryCategories {
        structured_metadata: merge_buf.split_off(w),
        parsed: Vec::new(),
    };
    // The reserved slots, under the same visibility gate the pipeline's
    // `ErrorSlots` applies at emit — but into `parsed`, not into the
    // stream object.
    sm_ctx.append_visible(&mut out.parsed);
    out.structured_metadata.sort_unstable();
    out.parsed.sort_unstable();
    merge_buf.sort_unstable();
    out
}

/// The pre-#312 page-at-a-time shape, reimplemented over
/// [`SmFanOutAccumulator`] so the shipped behaviour has exactly one
/// implementation.
///
/// **No production caller since issue #312** — the fast path pushes rows
/// into the accumulator as they arrive rather than staging `sm_rows` —
/// so this is `#[cfg(test)]`, exactly like
/// [`advance_tail_cursor`](super::exec::advance_tail_cursor). It keeps
/// the pre-#312 tests driving the shipped fan-out code with one extra
/// argument.
#[cfg(test)]
pub(in crate::logql) fn fan_out_sm_fast_path(
    sm_rows: &[SampleRow],
    meta: &HashMap<u64, StreamMetaRow>,
    budget: &mut StreamsResultBudget,
) -> Result<Vec<StreamResult>, ReadError> {
    let mut acc = SmFanOutAccumulator::default();
    for row in sm_rows {
        acc.push_row(row, meta, budget)?;
    }
    Ok(acc.into_streams())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::exec::advance_tail_cursor;
    use crate::logql::testkit::*;

    /// The house deterministic-seed PRNG (the xtask bench dataset
    /// pattern) — no rand dependency, reproducible failures.
    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The pre-#244 `feed_detected_rows` page shape, rebuilt over the
    /// streaming [`DetectedRowFeeder`] so the plan-v2 contract tests below
    /// drive the SHIPPED per-row path.
    fn feed_rows_via_feeder(
        rows: &[SampleRow],
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
        matched: &mut u32,
        line_limit: u32,
    ) -> Result<(), ReadError> {
        let mut feeder = DetectedRowFeeder::new();
        for row in rows {
            if *matched >= line_limit {
                break;
            }
            if feeder.feed_row(
                row.fingerprint,
                row.timestamp_ns,
                &row.body,
                &row.structured_metadata,
                base_labels,
                compiled,
                acc,
            )? {
                *matched += 1;
            }
        }
        Ok(())
    }

    /// Issue #170 plan v2 test delta 3: the detected-fields matched-entry
    /// count is POST-pipeline — rows the pipeline drops never count toward
    /// `line_limit`, and their fields are never observed.
    #[test]
    fn detected_fields_matched_count_is_post_pipeline_dropped_rows_do_not_count() {
        let expr = pulsus_logql::parse(r#"{app="x"} | json | level="rare""#).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&le.pipeline)
            .expect("compile pipeline");
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        let rows = vec![
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 3,
                body: r#"{"level":"common","code":1}"#.to_string(),
                structured_metadata: String::new(),
            },
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 2,
                body: "not json at all".to_string(),
                structured_metadata: String::new(),
            },
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: r#"{"level":"rare","code":7}"#.to_string(),
                structured_metadata: String::new(),
            },
        ];
        let mut acc = super::super::detected::FieldAccumulator::new(1000);
        let mut matched = 0u32;
        feed_rows_via_feeder(&rows, &base_labels, &compiled, &mut acc, &mut matched, 100)
            .expect("no budget breach");
        assert_eq!(
            matched, 1,
            "only the post-pipeline surviving row counts toward line_limit"
        );
        let (fields, _) = acc.finish();
        let labels: Vec<&str> = fields.iter().map(|f| f.label.as_str()).collect();
        assert_eq!(labels, vec!["code", "level"]);
        let code = fields.iter().find(|f| f.label == "code").expect("code");
        assert_eq!(code.field_type, "int");
        assert_eq!(
            code.cardinality, 1,
            "dropped rows' values are never observed"
        );
        assert_eq!(code.parsers, vec!["json"]);
    }

    /// Issue #170: the post-pipeline matched count stops feeding once
    /// `line_limit` survivors are collected (the fast path's cap).
    #[test]
    fn detected_fields_feed_stops_at_the_line_limit() {
        let expr = pulsus_logql::parse(r#"{app="x"}"#).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&le.pipeline)
            .expect("compile pipeline");
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        let rows: Vec<SampleRow> = (0..5)
            .map(|i| SampleRow {
                fingerprint: 1,
                timestamp_ns: i,
                body: format!(r#"{{"seq":"{i}"}}"#),
                structured_metadata: String::new(),
            })
            .collect();
        let mut acc = super::super::detected::FieldAccumulator::new(1000);
        let mut matched = 0u32;
        feed_rows_via_feeder(&rows, &base_labels, &compiled, &mut acc, &mut matched, 2)
            .expect("no budget breach");
        assert_eq!(matched, 2);
        let (fields, _) = acc.finish();
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].cardinality, 2,
            "rows past the line_limit are never sampled"
        );
    }

    // -- Issue #244: the streaming page-loop contract (AC 16) and the
    //    incremental cursor (AC 17), hermetic over injected pages. -------

    fn detected_compiled(query: &str) -> super::super::pipeline::CompiledPipeline {
        let expr = pulsus_logql::parse(query).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        super::super::pipeline::CompiledPipeline::compile(&le.pipeline).expect("compile")
    }

    fn detected_base_labels() -> HashMap<u64, Vec<(String, String)>> {
        let mut base_labels = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        base_labels
    }

    /// Distinct field name per row (the AC 16 fixture rule: a shared name
    /// would let a wrong drain produce the right field set).
    fn detected_tail_row(i: u64) -> TailSampleRow {
        TailSampleRow {
            fingerprint: 1,
            timestamp_ns: 1_000 - i as i64, // newest-first, all distinct
            body: format!(r#"{{"f{i}":{i}}}"#),
            body_hash: 0x9000 + i,
            structured_metadata: String::new(),
        }
    }

    fn scan_budget_err() -> ReadError {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: None,
        })
    }

    fn detected_paged_state(page_size: u32, line_limit: u32) -> DetectedPagedState {
        DetectedPagedState {
            feeder: DetectedRowFeeder::new(),
            cursor: None,
            spent: 0,
            matched: 0,
            page_size,
            line_limit,
            budget: u64::MAX,
        }
    }

    fn absorb(
        st: &mut DetectedPagedState,
        acc: &mut FieldAccumulator,
        compiled: &super::super::pipeline::CompiledPipeline,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        items: Vec<Result<TailSampleRow, ReadError>>,
        read_bytes: u64,
    ) -> Result<Option<bool>, ReadError> {
        let mut stream = futures::stream::iter(items);
        futures::executor::block_on(st.absorb_page(
            &mut stream,
            |_| Some(read_bytes),
            |e| e,
            base_labels,
            compiled,
            acc,
        ))
    }

    /// The error-free field set over `rows`, for the AC 16b comparisons.
    fn detected_fields_over(rows: &[TailSampleRow]) -> Vec<DetectedFieldOut> {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(u32::MAX, 100);
        let items: Vec<Result<TailSampleRow, ReadError>> = rows.iter().cloned().map(Ok).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 1);
        assert!(matches!(out, Ok(Some(false))), "error-free run terminates");
        acc.finish().0
    }

    /// AC 16a — the first-page rule: a `ScanBudgetBytes` error while
    /// draining the FIRST page (`spent == 0`) is `QueryTooBroad` (the
    /// end-to-end 422), REGARDLESS of how many rows were already
    /// delivered; the prefix is discarded with the request. Streaming
    /// must not turn that 422 into a 200.
    #[test]
    fn detected_paged_first_page_budget_error_stays_query_too_broad_despite_delivered_rows() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(10, 100);
        let items = vec![
            Ok(detected_tail_row(0)),
            Ok(detected_tail_row(1)),
            Ok(detected_tail_row(2)),
            Err(scan_budget_err()),
            Ok(detected_tail_row(3)),
            Ok(detected_tail_row(4)),
        ];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        match out {
            Err(ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })) => {}
            other => panic!("first-page budget error must propagate, got {other:?}"),
        }
    }

    /// AC 16b — the later-page rule: with `spent > 0`, a mid-page
    /// `ScanBudgetBytes` terminates PARTIAL with the accumulated prefix
    /// retained — provably the fields of `[r0, r1, r2]` (the delivered
    /// prefix), NOT of the last complete page alone (M6's page-atomic
    /// rewrite) and NOT of all five rows (M7's continue-past-error
    /// rewrite) — and the cursor covers exactly the three drained rows.
    #[test]
    fn detected_paged_later_page_budget_error_returns_partial_prefix_mid_page() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let rows: Vec<TailSampleRow> = (0..5).map(detected_tail_row).collect();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(2, 100);
        // Page 1: two rows, no error -> continue.
        let page1 = vec![Ok(rows[0].clone()), Ok(rows[1].clone())];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, page1, 64);
        assert!(matches!(out, Ok(None)), "page 1 continues: {out:?}");
        assert!(st.spent > 0, "page 1's read_bytes must be accounted");
        // Page 2: one row, then the budget error.
        let page2 = vec![
            Ok(rows[2].clone()),
            Err(scan_budget_err()),
            Ok(rows[3].clone()),
            Ok(rows[4].clone()),
        ];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, page2, 64);
        assert!(
            matches!(out, Ok(Some(true))),
            "later-page budget error terminates PARTIAL: {out:?}"
        );
        let got = acc.finish().0;
        assert_eq!(
            got,
            detected_fields_over(&rows[..3]),
            "(i) the retained prefix is exactly [r0, r1, r2]"
        );
        assert_ne!(
            got,
            detected_fields_over(&rows[..2]),
            "(ii) not the last complete page alone (M6)"
        );
        assert_ne!(
            got,
            detected_fields_over(&rows),
            "(iii) not all five rows (M7)"
        );
        // (iv) the cursor covers the three drained rows: pages [r0, r1]
        // then [r2].
        let want = advance_tail_cursor(
            advance_tail_cursor(None, &rows[..2]),
            std::slice::from_ref(&rows[2]),
        );
        assert_eq!(st.cursor, want, "cursor equals r2's tuple, 3 rows drained");
        assert_eq!(st.cursor.expect("cursor").tuple, {
            let r = &rows[2];
            (r.timestamp_ns, r.fingerprint, r.body_hash)
        });
    }

    /// AC 16c — the two COMPLETE terminals, plus `classify_page_error`'s
    /// truth table.
    #[test]
    fn detected_paged_terminal_branches_and_error_classification() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();

        // line_limit filled -> Ok(Some(false)), and the page still fully
        // drains (the cursor reaches the last raw row).
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(3, 1);
        let items = (0..3).map(|i| Ok(detected_tail_row(i))).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");
        assert_eq!(st.matched, 1);
        let r2 = detected_tail_row(2);
        assert_eq!(
            st.cursor.expect("cursor").tuple,
            (r2.timestamp_ns, r2.fingerprint, r2.body_hash),
            "line_limit stops FEEDING, never DRAINING"
        );

        // fetched < page_size (window exhausted) -> Ok(Some(false)).
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(10, 100);
        let items = (0..3).map(|i| Ok(detected_tail_row(i))).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");

        // classify_page_error's four cells.
        assert!(matches!(
            classify_page_error(scan_budget_err(), 1),
            Ok(true)
        ));
        assert!(classify_page_error(scan_budget_err(), 0).is_err());
        let other = || ReadError::PipelineInvalid {
            reason: "x".to_string(),
        };
        assert!(classify_page_error(other(), 1).is_err());
        assert!(classify_page_error(other(), 0).is_err());
    }

    /// AC 16d — the subset property: over >= 100 seeded sequences, an
    /// error-cut partial accumulation's label set is a SUBSET of the
    /// error-free run's, with at least one STRICT subset exercised.
    #[test]
    fn detected_paged_partial_prefix_is_always_a_subset_of_the_complete_answer() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut state: u64 = 0x0024_4bad_c0de;
        let mut strict_subsets = 0usize;
        for _ in 0..128 {
            let n = 1 + (splitmix64(&mut state) % 8) as usize;
            let cut = (splitmix64(&mut state) % n as u64) as usize;
            let rows: Vec<TailSampleRow> = (0..n as u64).map(detected_tail_row).collect();
            let full: std::collections::BTreeSet<String> = detected_fields_over(&rows)
                .into_iter()
                .map(|f| f.label)
                .collect();
            let mut acc = FieldAccumulator::new(1000);
            let mut st = detected_paged_state(u32::MAX, 100);
            st.spent = 1; // a later page, so the cut is PARTIAL not 422
            let mut items: Vec<Result<TailSampleRow, ReadError>> =
                rows.iter().take(cut).cloned().map(Ok).collect();
            items.push(Err(scan_budget_err()));
            items.extend(rows.iter().skip(cut).cloned().map(Ok));
            let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
            assert!(matches!(out, Ok(Some(true))), "{out:?}");
            let partial: std::collections::BTreeSet<String> =
                acc.finish().0.into_iter().map(|f| f.label).collect();
            assert!(
                partial.is_subset(&full),
                "partial {partial:?} must be a subset of {full:?}"
            );
            if partial.len() < full.len() {
                strict_subsets += 1;
            }
        }
        assert!(
            strict_subsets > 0,
            "at least one strict subset must be exercised"
        );
    }

    /// AC 17(a) — `TailCursorTracker` is EXACTLY `advance_tail_cursor`
    /// over randomized page sequences including empty pages, all-equal
    /// tuples and carry-from-`prev`; the drained count equals the page's
    /// row count.
    #[test]
    fn tail_cursor_tracker_matches_advance_tail_cursor_over_randomized_sequences() {
        let mut state: u64 = 0x1755;
        let mut prev: Option<TailCursor> = None;
        for round in 0..500 {
            let n = (splitmix64(&mut state) % 7) as usize; // 0 = empty page
            let all_equal = splitmix64(&mut state).is_multiple_of(4);
            let rows: Vec<TailSampleRow> = (0..n)
                .map(|i| {
                    // A tiny alphabet forces tie runs; `all_equal` pages
                    // force whole-page runs (the carry case).
                    let (ts, fp, h) = if all_equal {
                        (7, 7, 7)
                    } else {
                        (
                            (splitmix64(&mut state) % 3) as i64,
                            splitmix64(&mut state) % 2,
                            splitmix64(&mut state) % 2,
                        )
                    };
                    TailSampleRow {
                        fingerprint: fp,
                        timestamp_ns: ts,
                        body: format!("b{i}"),
                        body_hash: h,
                        structured_metadata: String::new(),
                    }
                })
                .collect();
            let want = advance_tail_cursor(prev, &rows);
            let mut tracker = TailCursorTracker::new();
            for r in &rows {
                tracker.observe(r.timestamp_ns, r.fingerprint, r.body_hash);
            }
            let (got, drained) = tracker.finish(prev);
            assert_eq!(got, want, "round {round}: rows {rows:?}");
            assert_eq!(drained as usize, rows.len(), "round {round}");
            prev = got;
        }
    }

    /// AC 17(b) — a page is drained PAST `line_limit`: feeding stops at
    /// the limit but the cursor still advances over the whole raw page.
    #[test]
    fn detected_paged_page_is_drained_past_the_line_limit() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(5, 1);
        let rows: Vec<TailSampleRow> = (0..5).map(detected_tail_row).collect();
        let items = rows.iter().cloned().map(Ok).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");
        assert_eq!(st.matched, 1, "feeding stopped at line_limit");
        assert_eq!(
            st.cursor,
            advance_tail_cursor(None, &rows),
            "the raw page is fully drained"
        );
        let fields = acc.finish().0;
        assert_eq!(fields.len(), 1, "only the fed row's field: {fields:?}");
    }

    // -- Issue #244 AC 12(c)–(e): the feeder's carried-capacity bound. ----

    /// AC 12(c) — after a row wider than `MAX_FEEDER_SCRATCH_SLOTS`
    /// pairs, every carried buffer is empty and capacity-capped, on the
    /// fed path AND the fingerprint-miss path.
    #[test]
    fn feeder_trim_caps_carried_capacity_after_a_wide_row_and_on_fingerprint_miss() {
        let compiled = detected_compiled(r#"{app="x"}"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(8);
        let mut feeder = DetectedRowFeeder::new();
        // A wide SM row: > MAX_FEEDER_SCRATCH_SLOTS pairs.
        let wide: usize = MAX_FEEDER_SCRATCH_SLOTS + 1000;
        let mut sm = String::from("{");
        for i in 0..wide {
            if i > 0 {
                sm.push(',');
            }
            sm.push_str(&format!(r#""k{i:05}":"v""#));
        }
        sm.push('}');
        let survived = feeder
            .feed_row(1, 1, "body", &sm, &base_labels, &compiled, &mut acc)
            .expect("no error");
        assert!(survived);
        let check = |feeder: &DetectedRowFeeder, ctx: &str| {
            assert_eq!(feeder.merge_buf.len(), 0, "{ctx}");
            assert_eq!(feeder.sm_buf.len(), 0, "{ctx}");
            assert_eq!(feeder.label_scratch.len(), 0, "{ctx}");
            assert!(
                feeder.merge_buf.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.sm_buf.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.label_scratch.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.sm_ctx.err.capacity() <= MAX_FEEDER_SCRATCH_STRING_BYTES,
                "{ctx}"
            );
            assert!(
                feeder.sm_ctx.details.capacity() <= MAX_FEEDER_SCRATCH_STRING_BYTES,
                "{ctx}"
            );
            assert!(
                feeder.scratch_capacity_bytes() <= MAX_FEEDER_SCRATCH_BYTES,
                "{ctx}"
            );
        };
        check(&feeder, "after the wide fed row");
        // The fingerprint-miss path trims too: pre-grow a buffer past the
        // cap, then feed a row whose fingerprint never hydrated.
        feeder.merge_buf = Vec::with_capacity(3 * MAX_FEEDER_SCRATCH_SLOTS);
        let survived = feeder
            .feed_row(999, 1, "body", "", &base_labels, &compiled, &mut acc)
            .expect("no error");
        assert!(!survived, "unknown fingerprint is skipped");
        check(&feeder, "after the fingerprint-miss row");
    }

    /// AC 12(d) — the carried-bound constant derives from `size_of` over
    /// exactly FIVE terms (a sixth feeder buffer — M10 — breaks this), and
    /// equals the plan-derived 64-bit figure.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn feeder_scratch_bound_is_the_five_term_size_of_derivation() {
        assert_eq!(MAX_FEEDER_SCRATCH_BYTES, 1_196_032);
        let pair = (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(String, String)>()) as u64;
        let cow =
            (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64;
        let five_terms = alloc_block_bytes(pair)
            + alloc_block_bytes(pair)
            + alloc_block_bytes(cow)
            + alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64)
            + alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64);
        assert_eq!(MAX_FEEDER_SCRATCH_BYTES, five_terms);
    }

    /// AC 12(e) — `recycle_label_scratch` preserves the allocation (the
    /// in-place `collect` specialization; M8's `Vec::new` replacement
    /// loses it).
    #[test]
    fn recycle_label_scratch_preserves_capacity() {
        let scratch: LabelScratch<'static> = Vec::with_capacity(1024);
        let recycled = recycle_label_scratch(scratch);
        assert_eq!(recycled.capacity(), 1024, "the allocation must be reused");
    }

    /// AC 13(e)'s compile half + a leak guard: the legacy helpers exist
    /// for the witness only. (The `git grep` reference audit is recorded
    /// in the #244 implementation notes; this test pins that the probe
    /// seam still routes production `feed_row` through the NEW shape by
    /// asserting the two paths agree on a smoke row.)
    ///
    /// `json_path` is the ONE field the two shapes are allowed to differ
    /// on (issue #254): the legacy transcription is frozen at the pre-#244
    /// text, which predates the capture, so it attributes `json` with no
    /// path. That asymmetry is asserted explicitly below rather than
    /// silently normalized away — if the production path ever STOPPED
    /// capturing, this test would fail on that assertion.
    #[test]
    fn probe_feed_row_and_legacy_shape_agree_on_a_smoke_row() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let mut new_probe = DetectedFieldsProbe::new(100, 1000);
        new_probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
        let mut legacy_probe = DetectedFieldsProbe::new(100, 1000);
        legacy_probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
        let body = r#"{"level":"info","code":7}"#;
        let sm = r#"{"trace_id":"abc"}"#;
        assert!(new_probe.feed_row(&compiled, 1, 5, body, sm).expect("ok"));
        assert!(
            legacy_probe
                .feed_row_legacy_shape(&compiled, 1, 5, body, sm)
                .expect("ok")
        );
        let (mut new_fields, new_capped) = new_probe.finish();
        let (legacy_fields, legacy_capped) = legacy_probe.finish();
        assert_eq!(
            new_fields
                .iter()
                .map(|f| (f.label.as_str(), f.json_path.clone()))
                .collect::<Vec<_>>(),
            vec![
                ("code", Some(vec!["code".to_string()])),
                ("level", Some(vec!["level".to_string()])),
                ("trace_id", None),
            ],
            "the production shape captures a json path per json-flattened field (#254)"
        );
        for f in &mut new_fields {
            f.json_path = None;
        }
        assert_eq!((new_fields, new_capped), (legacy_fields, legacy_capped));
    }

    /// The no-pipeline fast-path label set: merge + `append_visible` + sort
    /// — exactly what `fan_out_sm_fast_path` renders per row.
    fn fast_path_labels(base: &[(&str, &str)], sm_json: &str) -> Vec<(String, String)> {
        let base: Vec<(String, String)> = base
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut merge_buf = Vec::new();
        let mut sm_buf = Vec::new();
        let mut ctx = StructuredMetadataCtx::default();
        merge_labels_with_structured_metadata(
            &base,
            sm_json,
            &mut merge_buf,
            &mut sm_buf,
            &mut ctx,
        );
        ctx.append_visible(&mut merge_buf);
        merge_buf.sort();
        merge_buf
    }

    /// The bare-selector (no-pipeline) fast-path rows: C3, C5, C7, C10,
    /// C13, C14, C19, C25, C29 through `append_visible`'s gate — the same
    /// `visible()` rule the pipeline applies at emit, owned by this path
    /// because no `CompiledPipeline` runs here (kills W15, and W1/W2/W3/W7
    /// on this path).
    #[test]
    fn fast_path_bare_selector_rows_apply_the_materialisation_gate() {
        // C3: reserved err emits on a clean builder.
        assert_eq!(
            fast_path_labels(&[("service_name", "v2")], r#"{"__error__":"boom"}"#),
            owned_pairs(&[("__error__", "boom"), ("service_name", "v2")])
        );
        // C5: a lone details slot is INVISIBLE.
        assert_eq!(
            fast_path_labels(&[("service_name", "v3")], r#"{"__error_details__":"bdet"}"#),
            owned_pairs(&[("service_name", "v3")])
        );
        // C7: suffix-before-reserved (err branch).
        assert_eq!(
            fast_path_labels(
                &[("__error__", "streamerr"), ("service_name", "v10")],
                r#"{"__error__":"boom"}"#
            ),
            owned_pairs(&[
                ("__error__", "streamerr"),
                ("__error___extracted", "boom"),
                ("service_name", "v10"),
            ])
        );
        // C10/C14: empty reserved values contribute nothing.
        assert_eq!(
            fast_path_labels(&[("service_name", "v4")], r#"{"__error__":""}"#),
            owned_pairs(&[("service_name", "v4")])
        );
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v12")],
                r#"{"__error__":"","__error_details__":""}"#
            ),
            owned_pairs(&[("service_name", "v12")])
        );
        // C13: empty err + non-empty details, clean -> nothing.
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v6")],
                r#"{"__error__":"","__error_details__":"bdet"}"#
            ),
            owned_pairs(&[("service_name", "v6")])
        );
        // C19: details + ordinary dirt -> visible.
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v11")],
                r#"{"__error_details__":"bdet","trace_id":"abc"}"#
            ),
            owned_pairs(&[
                ("__error_details__", "bdet"),
                ("service_name", "v11"),
                ("trace_id", "abc"),
            ])
        );
        // C25: suffix-before-reserved (details branch) — both entries stay.
        assert_eq!(
            fast_path_labels(
                &[("__error_details__", "streamdet"), ("service_name", "v13")],
                r#"{"__error_details__":"smdet"}"#
            ),
            owned_pairs(&[
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
            ])
        );
        // C29 (kills W7 on this path): an empty ordinary entry does not
        // open the details gate (`trace_id=""` itself is #259).
        assert_eq!(
            fast_path_labels(
                &[("service_name", "w2")],
                r#"{"trace_id":"","__error_details__":"bdet"}"#
            ),
            owned_pairs(&[("service_name", "w2"), ("trace_id", "")])
        );
    }

    /// `fan_out_sm_fast_path` itself applies the gate (binding
    /// `append_visible` into the real path, not just the helper): the
    /// reserved-err row surfaces `__error__`, the reserved-details row
    /// surfaces nothing.
    #[test]
    fn fan_out_sm_fast_path_applies_the_reserved_sm_gate() {
        let mut meta = HashMap::new();
        meta.insert(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "v2".to_string(),
                labels: r#"{"service_name":"v2"}"#.to_string(),
            },
        );
        meta.insert(
            2u64,
            StreamMetaRow {
                fingerprint: 2,
                service: "v3".to_string(),
                labels: r#"{"service_name":"v3"}"#.to_string(),
            },
        );
        let rows = vec![
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: "a=Hello b=World".to_string(),
                structured_metadata: r#"{"__error__":"boom"}"#.to_string(),
            },
            SampleRow {
                fingerprint: 2,
                timestamp_ns: 2,
                body: "a=Hello b=World".to_string(),
                structured_metadata: r#"{"__error_details__":"bdet"}"#.to_string(),
            },
        ];
        let mut budget = StreamsResultBudget::new();
        let mut got: Vec<String> = fan_out_sm_fast_path(&rows, &meta, &mut budget)
            .expect("well inside the shipped streams result budget")
            .into_iter()
            .map(|s| s.labels_json)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                r#"{"__error__":"boom","service_name":"v2"}"#.to_string(),
                r#"{"service_name":"v3"}"#.to_string(),
            ]
        );
    }
}
