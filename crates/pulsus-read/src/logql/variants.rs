//! `variants(...) of (...)` — the fan-out aggregation of issue #221.
//!
//! [`VariantArena`] interns the sub-query pipelines a variants query
//! fans out to, [`VariantsAggState`] drives one sub-state per variant,
//! and the byte model here prices the fan-out before it exists so a wide
//! query is refused rather than served from a swollen heap.

use super::error::{ReadError, TooBroadReason};
use super::pipeline::CompiledPipeline;
use super::plan::{self};
use super::rows::{MetricScanRow, StreamMetaRow};
use pulsus_logql::{
    Grouping, LabelFilterExpr, LabelFmt, LineFilterOp, MatchOp, Matcher, MetricExpr,
    NumericLiteral, ParserStage, RangeAggOp, Stage, StreamSelector,
};
use std::collections::HashMap;
use std::ops::ControlFlow;

use super::agg::LabelSet;
use super::charge::{
    AggCaps, alloc_block_bytes, grown_alloc_bytes, label_set_bytes, map_entry_bytes,
    result_series_breach,
};
use super::client_agg::{ClientAggState, MetricAggState, RangeSlideState};
use super::exec::{MatrixSeries, QueryResult, VectorSample};
use super::post_agg::apply_vector_aggs;
use super::warnings::{Warnings, variant_series_warning};
use super::window::{ClientWindow, grid_point_count};

/// The reference's variant-index label (`__variant__`), set to the plain
/// decimal `index.to_string()` — no padding.
pub const VARIANT_LABEL: &str = "__variant__";

/// The variants fan-out state budget — DERIVED, not chosen: one extra
/// query-lifetime group-bytes budget ([`AggCaps::DEFAULT`]`.group_bytes`
/// == [`MAX_CLIENT_AGG_GROUP_BYTES`]). It moves with that cap.
///
/// **One COUNTER, a SECOND ceiling** (corrected by issue #260; the
/// former wording, "the budget is never doubled", was false). ONE counter
/// spans plan-time spec state and exec-time arena/sub-state state — that
/// is the property this constant guarantees, and it is what
/// `charge_fanout_bytes`'s single `charged` field enforces. But that
/// counter sits BESIDE the sub-states' own group-byte counters (each on
/// [`AggCaps::divided`]`(n)`, summing to the whole), so a variants query
/// holds two live counters against the group-byte cap's VALUE — the
/// `group_bytes: 2` row of [`super::charge::CounterPlurality`], and the
/// reason the composed bound
/// ([`super::charge::MAX_LEAF_RETAINED_BYTES`]) counts this cap twice.
/// The variants leaf is still strictly the narrower of the two shapes it
/// covers (no fold, and every other cap divided).
pub const MAX_VARIANT_FANOUT_STATE_BYTES: u64 = AggCaps::DEFAULT.group_bytes;

/// A CLONE of a source stage list, per SOURCE byte `S`
/// ([`stage_source_bytes`]): content ≤ 2S ([`alloc_block_bytes`]'s
/// size-class model) + per-allocation floor ≤ 32S ([`MIN_ALLOC_BYTES`];
/// each allocation needs ≥ 1 source byte — an empty-string clone
/// allocates nothing) + inner element slots
/// ≤ 2 × size_of::<(String, String)>() × S = 96S ⇒ ≤ 130S. Over-charging
/// is the safe direction: a breach is a clean 422, never an OOM.
const STAGE_CLONE_BYTES_PER_SOURCE_BYTE: u64 = 130;

/// A cloned/compiled stage list's retained heap per SOURCE byte — the
/// clone factor above plus the `Vec<CompiledStage>` slot term (the widest
/// arm is `Regexp(regex::Regex)`, so the slot width is DERIVED from the
/// real private enum via [`super::pipeline::COMPILED_STAGE_SLOT_BYTES`],
/// never hard-coded: a flat literal silently under-charges if the enum
/// widens). ≤ 2 slots per source byte covers the exactly-reserved buffer
/// at the [`alloc_block_bytes`] 2× rounding.
const COMPILED_STAGE_BYTES_PER_SOURCE_BYTE: u64 =
    STAGE_CLONE_BYTES_PER_SOURCE_BYTE + 2 * super::pipeline::COMPILED_STAGE_SLOT_BYTES as u64;

/// One `regex::Regex` CLONE's own retained heap: a fresh, lazily
/// populated cache pool — the lazy-DFA cache capacity (2 MiB, the regex
/// crate's default `hybrid_cache_capacity`) plus the meta engine's other
/// per-`Cache` structures (one-pass DFA, backtracker visited set,
/// PikeVM). The compiled PROGRAM is shared through the `Arc`
/// ([`CompiledPipeline::extended_with`]) and is NOT charged again.
const REGEX_CACHE_STATE_BYTES: u64 = 4 * 1024 * 1024;

/// A provable UPPER BOUND on the heap ONE **exactly reserved**
/// (`Vec::with_capacity(n)`) buffer of `n` `T` slots retains. The payload
/// behind any pointer inside `T` is charged SEPARATELY (the C/H split) —
/// this is the container line only.
///
/// No growth term is needed *because* every N-scaled buffer this path
/// introduces is `with_capacity`-reserved before the first push, with `n`
/// known up front; there is no realloc peak. A buffer whose count is NOT
/// known up front must use [`grown_alloc_bytes`] instead
/// (`Grouping::labels`, which `__variant__` is pushed into, does).
pub(crate) fn vec_buffer_bytes<T>(n: u64) -> u64 {
    alloc_block_bytes(n.saturating_mul(size_of::<T>() as u64))
}

/// The four driver-owned N-scaled BUFFERS, charged together as one term
/// BEFORE the first of them is allocated: [`VariantArena`]'s
/// `pipelines`/`slot` and [`VariantsAggState`]'s `subs`/`sub_charged`.
/// Charged in one place because `VariantsAggState` inherits the arena's
/// charge as its floor (`base`), so a single charge at the top of
/// [`VariantArena::build`] provably precedes every one of the four
/// allocations.
pub(in crate::logql) fn variant_driver_buffer_bytes(n: u64) -> u64 {
    vec_buffer_bytes::<usize>(n)
        .saturating_add(vec_buffer_bytes::<CompiledPipeline>(n.saturating_add(1)))
        .saturating_add(vec_buffer_bytes::<MetricAggState<'_>>(n))
        .saturating_add(vec_buffer_bytes::<u64>(n))
}

/// Total OWNED-STRING bytes in a SOURCE stage list — the `S` the clone
/// factors above multiply. An exhaustive `match` with **no `_` arm**: a
/// new `Stage` variant is a compile error here, forcing the sizing walk
/// to be re-run for it before it can ship.
pub(crate) fn stage_source_bytes(stages: &[Stage]) -> u64 {
    fn matcher_bytes(m: &Matcher) -> u64 {
        (m.name.len() as u64).saturating_add(m.value.len() as u64)
    }
    /// Issue #272: a driver consumer. Summing over a pre-order walk is
    /// the same total as summing over the recursion it replaces.
    fn label_filter_bytes(e: &LabelFilterExpr) -> u64 {
        let mut bytes: u64 = 0;
        pulsus_logql::for_each_label_filter(e, |n| {
            bytes = bytes.saturating_add(match n {
                LabelFilterExpr::Match(m) => matcher_bytes(m),
                LabelFilterExpr::Compare { name, rhs, .. } => {
                    let rhs_len = match rhs {
                        NumericLiteral::Number(raw) | NumericLiteral::DurationOrBytes(raw) => {
                            raw.len() as u64
                        }
                    };
                    (name.len() as u64).saturating_add(rhs_len)
                }
                LabelFilterExpr::Ip { name, value, .. } => {
                    (name.len() as u64).saturating_add(value.len() as u64)
                }
                LabelFilterExpr::And(..) | LabelFilterExpr::Or(..) => 0,
            });
        });
        bytes
    }
    let mut bytes: u64 = 0;
    for stage in stages {
        let stage_bytes = match stage {
            Stage::LineFilter(lf) => lf
                .alternatives()
                .fold(0u64, |acc, (v, _)| acc.saturating_add(v.len() as u64)),
            Stage::Parser(p) => match p {
                ParserStage::Json { extractions } => extractions.iter().fold(0u64, |acc, e| {
                    acc.saturating_add(e.label.len() as u64)
                        .saturating_add(e.expression.len() as u64)
                }),
                ParserStage::Logfmt { extractions, .. } => {
                    extractions.iter().fold(0u64, |acc, e| {
                        acc.saturating_add(e.label.len() as u64)
                            .saturating_add(e.expression.len() as u64)
                    })
                }
                ParserStage::Regexp(raw) | ParserStage::Pattern(raw) => raw.len() as u64,
            },
            Stage::LabelFilter(expr) => label_filter_bytes(expr),
            Stage::LineFormat(tmpl) => tmpl.len() as u64,
            Stage::LabelFormat(fmts) => fmts.iter().fold(0u64, |acc, f| match f {
                LabelFmt::Rename { dst, src } => acc
                    .saturating_add(dst.len() as u64)
                    .saturating_add(src.len() as u64),
                LabelFmt::Template { dst, tmpl } => acc
                    .saturating_add(dst.len() as u64)
                    .saturating_add(tmpl.len() as u64),
            }),
            Stage::Unwrap(u) => (u.label.len() as u64)
                .saturating_add(u.conversion.as_deref().map_or(0, |c| c.len() as u64)),
            Stage::Unpack | Stage::Decolorize => 0,
            Stage::Drop(elems) | Stage::Keep(elems) => elems.iter().fold(0u64, |acc, e| {
                acc.saturating_add(e.label.len() as u64)
                    .saturating_add(e.matcher.as_ref().map_or(0, |m| m.value.len() as u64))
            }),
        };
        bytes = bytes.saturating_add(stage_bytes);
    }
    bytes
}

/// Regex-compiling stage forms in a SOURCE list (the compiled internals
/// are private to `pipeline.rs`), enumerated from the five
/// `compile_regex`/`compile_anchored_regex` call sites: `LineFilter`
/// alternatives under `|~`/`!~` (non-`ip` heads and `or` alternatives),
/// `| regexp`, label-filter `Match` nodes with `=~`/`!~` (recursing
/// through `and`/`or`), `| decolorize`, and `drop`/`keep` elements with a
/// `=~`/`!~` matcher.
pub(crate) fn regex_stage_count(stages: &[Stage]) -> u64 {
    /// Issue #272: a driver consumer, same total as the recursion.
    fn label_filter_regexes(e: &LabelFilterExpr) -> u64 {
        let mut count: u64 = 0;
        pulsus_logql::for_each_label_filter(e, |n| {
            count = count.saturating_add(match n {
                LabelFilterExpr::Match(m) => u64::from(matches!(m.op, MatchOp::Re | MatchOp::Nre)),
                LabelFilterExpr::Compare { .. }
                | LabelFilterExpr::Ip { .. }
                | LabelFilterExpr::And(..)
                | LabelFilterExpr::Or(..) => 0,
            });
        });
        count
    }
    let mut count: u64 = 0;
    for stage in stages {
        let stage_count = match stage {
            Stage::LineFilter(lf)
                if matches!(lf.op, LineFilterOp::Regex | LineFilterOp::NotRegex) =>
            {
                lf.alternatives().fold(0u64, |acc, (_, is_ip)| {
                    acc.saturating_add(u64::from(!is_ip))
                })
            }
            Stage::LineFilter(_) => 0,
            Stage::Parser(ParserStage::Regexp(_)) => 1,
            Stage::Parser(_) => 0,
            Stage::LabelFilter(expr) => label_filter_regexes(expr),
            Stage::Decolorize => 1,
            Stage::Drop(elems) | Stage::Keep(elems) => elems.iter().fold(0u64, |acc, e| {
                acc.saturating_add(u64::from(
                    e.matcher
                        .as_ref()
                        .is_some_and(|m| matches!(m.op, MatchOp::Re | MatchOp::Nre)),
                ))
            }),
            Stage::LineFormat(_) | Stage::LabelFormat(_) | Stage::Unwrap(_) | Stage::Unpack => 0,
        };
        count = count.saturating_add(stage_count);
    }
    count
}

/// The charge for ONE additional [`VariantArena`] entry (a distinct
/// non-empty unwrap tail): the cloned common stages + compiled tail
/// (source-byte factors) plus one fresh regex cache pool per regex in
/// EITHER list — the compiled programs themselves are `Arc`-shared by
/// [`CompiledPipeline::extended_with`] and never recompiled.
pub(crate) fn variant_pipeline_entry_bytes(common: &[Stage], tail: &[Stage]) -> u64 {
    stage_source_bytes(common)
        .saturating_add(stage_source_bytes(tail))
        .saturating_mul(COMPILED_STAGE_BYTES_PER_SOURCE_BYTE)
        .saturating_add(
            regex_stage_count(common)
                .saturating_add(regex_stage_count(tail))
                .saturating_mul(REGEX_CACHE_STATE_BYTES),
        )
}

/// EXACT bytes one variants sub-state's construction-time `meta` snapshot
/// retains, with the C/H split explicit — per map, the TABLE share and
/// the element payload are separate terms:
///
/// - `base_labels`: `entries × map_entry_bytes(size_of::<(u64, LabelSet)>())`
///   (C) + `Σ label_set_bytes(labels)` (H);
/// - `hashes`: `entries × map_entry_bytes(size_of::<(u64, u64)>())`
///   (C; the payload is `Copy`).
///
/// **The `hashes` term is unconditional since issue #344.** It used to be
/// charged for the sliding kind only, because only that kind held the
/// map; the instant kind now holds one too — `first_over_time`/
/// `last_over_time` take the endpoints of Loki's
/// `(timestamp, stream_hash, tie_rank)` order on BOTH paths, so both
/// need the per-fingerprint `StableHash`. The `with_hashes` parameter is
/// gone rather than passed `true` twice: a flag with one reachable value
/// is a place for the two kinds to silently drift apart again.
///
/// Walked over the FIRST sub-state's ALREADY-BUILT maps, so the sizing
/// pass is one O(streams) traversal with no re-parse and no allocation,
/// and runs only when a query declares ≥ 2 variants.
fn variant_meta_snapshot_bytes(base_labels: &HashMap<u64, LabelSet>) -> u64 {
    let mut bytes: u64 = 0;
    for labels in base_labels.values() {
        bytes = bytes
            .saturating_add(map_entry_bytes(size_of::<(u64, LabelSet)>()))
            .saturating_add(label_set_bytes(labels));
    }
    bytes.saturating_add(
        (base_labels.len() as u64).saturating_mul(map_entry_bytes(size_of::<(u64, u64)>())),
    )
}

/// The per-sub-state charge, for sub-state index ≥ 1 (index 0 costs
/// exactly what the equivalent single-extractor query already allocates
/// and is charged ZERO — so a 1-variant query is admitted exactly when
/// the plain query is): the boxed state slot (the kind actually built),
/// the `meta` snapshot, the range kind's construction-time
/// `absent_labels` clone (zero when the list is empty — an empty `Vec`
/// clone allocates nothing), and `present_cover` for an
/// `absent_over_time` range sub-state (structurally absent otherwise).
pub(in crate::logql) fn variant_state_bytes(
    is_range: bool,
    is_absent: bool,
    absent_labels: &LabelSet,
    meta_bytes: u64,
    kmax: i64,
) -> u64 {
    let slot = if is_range {
        alloc_block_bytes(size_of::<RangeSlideState<'_>>() as u64)
    } else {
        alloc_block_bytes(size_of::<ClientAggState<'_>>() as u64)
    };
    let mut bytes = slot.saturating_add(meta_bytes);
    if is_range && !absent_labels.is_empty() {
        bytes = bytes.saturating_add(label_set_bytes(absent_labels));
    }
    if is_range && is_absent {
        bytes = bytes.saturating_add(alloc_block_bytes(8 * (kmax.max(-1) + 2) as u64));
    }
    bytes
}

/// Every byte one [`plan::VariantSpec`] retains, sized from BORROWED AST
/// pieces the planner already holds — no container is constructed to
/// compute this bound, and the whole charge precedes the first clone
/// (issue #221 review rounds 4–5).
///
/// **The walk that produced this bound (W-MEM, verbatim so it is
/// re-runnable).** The accounting domain is the OWNED-TYPE CLOSURE of the
/// per-variant roots (`VariantSpec`, `VariantArena`, `VariantsAggState`,
/// `ClientAggState`, `RangeSlideState`) **plus their construction path**
/// (every function executed en route to producing them, walked in call
/// order, each allocating statement classified). Buckets: (S) `Copy`
/// scalar in the slot; (B) borrowed; (C) container buffer/table — charged
/// via [`vec_buffer_bytes`]/[`map_entry_bytes`]/[`grown_alloc_bytes`];
/// (H) element payload — charged before allocation; (G) grows during the
/// scan — governed by a **divided** [`AggCaps`] field. Residues (an
/// explicit byte bound, or N-neutrality, never a bare count): **R1** the
/// once-per-query common-range plan products (one synthetic
/// `MetricExpr::Range` + `metric_plan`'s usual allocations — zero slope
/// in N, so outside a per-variant boundary); **R2** per-fingerprint
/// transients inside the sub-state constructors (`series_labels`
/// scratch) — sub-states are constructed sequentially, so the peak is
/// N-independent and the retained result is charged by
/// [`variant_meta_snapshot_bytes`]; **R4** the three no-count-band
/// branches (meta hydration, a distinct-tail arena compile, an
/// aggregation-bearing `apply_vector_aggs`), named with their
/// compensating controls in `tests/logql_variants_alloc.rs`.
///
/// Terms, in table order (each isolated by a one-axis fixture pair in
/// the I-series tests):
/// - the tail buffer (C) + its `Stage` payload (H, the clone factor);
/// - the `absent_labels` buffer + strings (C+H, `absent_over_time` only —
///   Δ2: sourced from the VARIANT's own selector);
/// - the `vector_aggs` buffer (C — [`grown_alloc_bytes`], because
///   `Result<Vec<_>>`-collect grows by pushes, never one reservation);
/// - per vector layer, grouping present OR CREATED (member M3):
///   the `Grouping::labels` buffer ([`grown_alloc_bytes`] over `len + 1` —
///   `__variant__` is PUSHED into it, the one N-scaled buffer that
///   reallocs) + each cloned label + the injected `__variant__` string.
/// - the variant's own RANGE grouping (issue #344): the `Box` holding the
///   normalized clause, plus `RangeGrouping::from_ast`'s clone of the
///   AST's label list — an exactly-reserved buffer and one owned `String`
///   per name (the sort and dedup that follow are in place and allocate
///   nothing). Charged whether or not the dedup shrinks the list, because
///   the clone happens first.
pub(crate) fn variant_spec_bytes(
    tail: &[Stage],
    selector: &StreamSelector,
    is_absent: bool,
    range_grouping: Option<&Grouping>,
    agg_chain: &MetricExpr,
) -> u64 {
    let mut bytes: u64 = 0;
    if let Some(g) = range_grouping {
        bytes = bytes.saturating_add(alloc_block_bytes(
            size_of::<super::pipeline::RangeGrouping>() as u64,
        ));
        if !g.labels.is_empty() {
            bytes = bytes.saturating_add(vec_buffer_bytes::<String>(g.labels.len() as u64));
            for l in &g.labels {
                bytes = bytes.saturating_add(alloc_block_bytes(l.len() as u64));
            }
        }
    }
    if !tail.is_empty() {
        bytes = bytes
            .saturating_add(vec_buffer_bytes::<Stage>(tail.len() as u64))
            .saturating_add(
                stage_source_bytes(tail).saturating_mul(STAGE_CLONE_BYTES_PER_SOURCE_BYTE),
            );
    }
    if is_absent {
        let mut eq_count: u64 = 0;
        for m in selector.matchers.iter().filter(|m| m.op == MatchOp::Eq) {
            eq_count += 1;
            bytes = bytes
                .saturating_add(alloc_block_bytes(m.name.len() as u64))
                .saturating_add(alloc_block_bytes(m.value.len() as u64));
        }
        if eq_count > 0 {
            bytes = bytes.saturating_add(vec_buffer_bytes::<(String, String)>(eq_count));
        }
    }
    let mut layers: u64 = 0;
    // Issue #272: an allocation-free spine descent over the vector chain.
    let _ = pulsus_logql::walk::descend_spine::<pulsus_logql::MetricScc, ()>(
        pulsus_logql::MeNode::Expr(agg_chain),
        |n| {
            let pulsus_logql::MeNode::Expr(MetricExpr::Vector { grouping, .. }) = n else {
                return ControlFlow::Break(());
            };
            layers += 1;
            let declared = grouping.as_ref().map_or(0, |g| g.labels.len() as u64);
            bytes = bytes.saturating_add(grown_alloc_bytes(
                (declared + 1).saturating_mul(size_of::<String>() as u64),
            ));
            if let Some(g) = grouping {
                for l in &g.labels {
                    bytes = bytes.saturating_add(alloc_block_bytes(l.len() as u64));
                }
            }
            bytes = bytes.saturating_add(alloc_block_bytes(VARIANT_LABEL.len() as u64));
            ControlFlow::Continue(())
        },
    );
    if layers > 0 {
        bytes = bytes.saturating_add(grown_alloc_bytes(
            layers.saturating_mul(size_of::<plan::VectorAggSpec>() as u64),
        ));
    }
    bytes
}

/// The one variants fan-out gate: every charged allocation is sized,
/// checked against the cap, and accounted HERE — before the allocation it
/// pays for. Mirrors [`charge_group_bytes`] exactly (same shape, same
/// `saturating_add` rationale, same post-condition); returns the
/// `(next, cap)` pair on breach so each caller wraps it in ITS reason
/// ([`TooBroadReason::VariantSpecBytes`] at plan time,
/// [`TooBroadReason::VariantStateBytes`] at exec time) without a second
/// implementation.
pub(crate) fn charge_fanout_bytes(
    charged: &mut u64,
    bytes: u64,
    cap: u64,
) -> Result<(), (u64, u64)> {
    let next = charged.saturating_add(bytes);
    if next > cap {
        return Err((next, cap));
    }
    *charged = next;
    Ok(())
}

/// Releases a [`charge_fanout_bytes`] charge as `finish` consumes the
/// state it paid for. Saturating for panic-proofing only — the pairing is
/// exact, which `finish`'s `charged == base` post-condition asserts.
fn discharge_fanout_bytes(charged: &mut u64, bytes: u64) {
    *charged = charged.saturating_sub(bytes);
}

fn variant_state_breach((bytes, cap): (u64, u64)) -> ReadError {
    ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { bytes, cap })
}

/// The compiled-pipeline arena for ONE variants query (issue #221).
/// Entry 0 is the COMMON pipeline; each further entry is
/// `entry0.extended_with(tail)` for one DISTINCT non-empty unwrap tail.
/// Built and OWNED by the driver BEFORE [`VariantsAggState`], which only
/// BORROWS from it — the arena and its borrowers are never owned by the
/// same struct, so there is no self-reference.
#[derive(Debug)]
pub struct VariantArena {
    pipelines: Vec<CompiledPipeline>,
    slot: Vec<usize>,
    charged: u64,
}

impl VariantArena {
    /// ORDER (normative — every allocation is preceded by its charge):
    /// 1. count gate (defense in depth — the planner already gated it;
    ///    `build` is reachable from tests and the pure
    ///    [`run_variants_rows`] seam);
    /// 2. ONE [`variant_driver_buffer_bytes`] charge covering all four
    ///    driver buffers, levied before any of them exists
    ///    (`base_charged` = the planner's `spec_bytes`, so plan-time and
    ///    exec-time share one counter and one cap);
    /// 3. the two exact reservations;
    /// 4. entry 0 (`compile(common)`), charged ZERO — a single-extractor
    ///    query pays it too;
    /// 5. per variant with a NON-EMPTY tail, in index order: dedup by
    ///    backward scan over the TAIL slices already processed (no
    ///    auxiliary container, and `common ++ tail` is NEVER materialized
    ///    to compare — that would be one allocation per variant); on a
    ///    miss, charge [`variant_pipeline_entry_bytes`] BEFORE
    ///    `extended_with`.
    pub fn build(
        common: &[Stage],
        variants: &[plan::VariantSpec],
        cap: u64,
        base_charged: u64,
    ) -> Result<Self, ReadError> {
        let n = variants.len() as u64;
        if n > plan::MAX_VARIANT_SUB_STATES {
            return Err(ReadError::QueryTooBroad(TooBroadReason::VariantSubStates {
                count: n,
                cap: plan::MAX_VARIANT_SUB_STATES,
            }));
        }
        let mut charged = base_charged;
        // AC 14: at N = 1 the arena charge is exactly 0 — a 1-variant
        // query is admitted exactly when the equivalent single-extractor
        // query is; the driver buffers are charged only when the fan-out
        // is real (N >= 2).
        if n >= 2 {
            charge_fanout_bytes(&mut charged, variant_driver_buffer_bytes(n), cap)
                .map_err(variant_state_breach)?;
        }
        let mut pipelines: Vec<CompiledPipeline> = Vec::with_capacity(variants.len() + 1);
        let mut slot: Vec<usize> = Vec::with_capacity(variants.len());
        pipelines.push(CompiledPipeline::compile(common)?);
        for (i, spec) in variants.iter().enumerate() {
            let tail = &spec.client().pipeline;
            if tail.is_empty() {
                slot.push(0);
                continue;
            }
            // Dedup key is the variant's TAIL slice alone: entry_i ==
            // entry0.extended_with(tail_i), so equal tails give equal
            // entries (the common prefix is fixed for the query).
            let mut shared = None;
            for j in 0..i {
                if variants[j].client().pipeline == *tail {
                    shared = Some(slot[j]);
                    break;
                }
            }
            if let Some(s) = shared {
                slot.push(s);
                continue;
            }
            charge_fanout_bytes(
                &mut charged,
                variant_pipeline_entry_bytes(common, tail),
                cap,
            )
            .map_err(variant_state_breach)?;
            let extended = pipelines[0].extended_with(tail)?;
            pipelines.push(extended);
            slot.push(pipelines.len() - 1);
        }
        Ok(VariantArena {
            pipelines,
            slot,
            charged,
        })
    }

    /// The compiled pipeline variant `variant_index` runs (`common ++
    /// tail`, shared for empty/duplicate tails).
    fn get(&self, variant_index: usize) -> &CompiledPipeline {
        &self.pipelines[self.slot.get(variant_index).copied().unwrap_or(0)]
    }

    pub fn charged_bytes(&self) -> u64 {
        self.charged
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }
}

/// The variants driver: ONE scan, N sub-states (issue #221). Owns the
/// per-sub-state charges and releases them as `finish` consumes each
/// sub-state.
#[derive(Debug)]
pub struct VariantsAggState<'q> {
    arena: &'q VariantArena,
    variants: &'q [plan::VariantSpec],
    subs: Vec<MetricAggState<'q>>,
    sub_charged: Vec<u64>,
    /// Running fan-out charge; starts at (and returns to) `base`.
    charged: u64,
    /// The arena's charge (which already includes the planner's
    /// `spec_bytes` and the driver buffers) — the floor `finish` returns
    /// to.
    base: u64,
    cap: u64,
}

impl<'q> VariantsAggState<'q> {
    /// Builds sub-state 0 uncharged (a 1-variant query is admitted
    /// exactly when the equivalent single-extractor query is), sizes the
    /// `meta` snapshot from ITS already-built maps, then charges
    /// [`variant_state_bytes`] BEFORE constructing each further
    /// sub-state. The state kind comes from each spec's window (all
    /// variants share the grid, so all are the same kind); every
    /// sub-state gets `AggCaps::DEFAULT.divided(n)`, so the per-field SUM
    /// over sub-states is exactly the single-query bound.
    pub fn new(
        arena: &'q VariantArena,
        variants: &'q [plan::VariantSpec],
        meta: &HashMap<u64, StreamMetaRow>,
        cap: u64,
    ) -> Result<Self, ReadError> {
        let n = variants.len() as u64;
        let caps = AggCaps::DEFAULT.divided(n.max(1));
        let base = arena.charged_bytes();
        let mut charged = base;
        let mut subs: Vec<MetricAggState<'q>> = Vec::with_capacity(variants.len());
        let mut sub_charged: Vec<u64> = Vec::with_capacity(variants.len());
        let mut meta_bytes: u64 = 0;
        for (i, spec) in variants.iter().enumerate() {
            let compiled = arena.get(i);
            let is_range = matches!(spec.window(), ClientWindow::Range { .. });
            let is_absent = matches!(spec.client().range_op, RangeAggOp::AbsentOverTime);
            if i > 0 {
                let kmax = match spec.window() {
                    ClientWindow::Range {
                        grid_start_ns,
                        end_ns,
                        step_ns,
                        ..
                    } => grid_point_count(grid_start_ns, end_ns, step_ns.as_u64()) as i64 - 1,
                    ClientWindow::Instant { .. } => -1,
                };
                let bytes = variant_state_bytes(
                    is_range,
                    is_absent,
                    &spec.client().absent_labels,
                    meta_bytes,
                    kmax,
                );
                charge_fanout_bytes(&mut charged, bytes, cap).map_err(variant_state_breach)?;
                sub_charged.push(bytes);
            } else {
                sub_charged.push(0);
            }
            let state = if is_range {
                MetricAggState::Range(Box::new(RangeSlideState::new(
                    compiled,
                    meta,
                    spec.client(),
                    spec.window(),
                    spec.rate_window_ns(),
                    caps,
                )?))
            } else {
                let instant =
                    spec.window()
                        .as_instant()
                        .ok_or_else(|| ReadError::PipelineInvalid {
                            reason: "internal: a stepped variant window reached the instant \
                                     aggregation state"
                                .to_string(),
                        })?;
                MetricAggState::Instant(Box::new(ClientAggState::new(
                    compiled,
                    meta,
                    spec.client(),
                    instant,
                    spec.rate_window_ns(),
                    caps,
                )?))
            };
            if i == 0 {
                // Sized from the FIRST sub-state's already-built maps —
                // one O(streams) walk, no re-parse, no allocation. Both
                // kinds carry `base_labels` AND `hashes` since issue
                // #344, so the two arms differ only in which state they
                // borrow from.
                meta_bytes = match &state {
                    MetricAggState::Range(s) => variant_meta_snapshot_bytes(&s.base_labels),
                    MetricAggState::Instant(s) => variant_meta_snapshot_bytes(&s.base_labels),
                };
            }
            subs.push(state);
        }
        Ok(VariantsAggState {
            arena,
            variants,
            subs,
            sub_charged,
            charged,
            base,
            cap,
        })
    }

    /// Test/gate accessor for the measured-vs-charged slope gates.
    pub fn charged_bytes(&self) -> u64 {
        self.charged
    }

    /// How many vector aggregations the sub-states have taken over —
    /// zero, always (issue #260): the fan-out never calls `attach_fold`,
    /// so a variants query's live group-byte counters are
    /// `{fan-out, Σ sub-states}` and NOT a third fold counter, which is
    /// what keeps [`super::charge::LEAF_COUNTERS`]`.group_bytes` at 2.
    /// Asserted on a really-constructed state by
    /// `the_variants_sub_states_take_no_fold`.
    #[cfg(test)]
    pub(in crate::logql) fn sub_folded_aggs(&self) -> usize {
        self.subs.iter().map(MetricAggState::folded_aggs).sum()
    }

    /// Forwards one chunk to every sub-state in INDEX ORDER (so a
    /// surviving-`__error__` failure is raised by the lowest-indexed
    /// variant — deterministic). A Range sub-state receives the whole
    /// slice (its own sliding `(t-range, t]` windows apply the variant's
    /// `[range]`); an Instant sub-state receives maximal in-window runs
    /// of the SAME slice (no per-variant buffer): the scan is bounded by
    /// the COMMON range only, so a variant with a shorter `[range]` must
    /// exclude the older rows here.
    pub fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        for (i, sub) in self.subs.iter_mut().enumerate() {
            match sub {
                MetricAggState::Range(s) => s.push_rows(rows)?,
                MetricAggState::Instant(s) => {
                    let spec = &self.variants[i];
                    let mut k = 0usize;
                    while k < rows.len() {
                        if !spec.admits_instant(rows[k].timestamp_ns) {
                            k += 1;
                            continue;
                        }
                        let start = k;
                        while k < rows.len() && spec.admits_instant(rows[k].timestamp_ns) {
                            k += 1;
                        }
                        s.push_rows(&rows[start..k])?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The finish body, in place so the `charged == base` post-condition
    /// is OBSERVABLE (the `RangeSlideState::finish_in_place` precedent).
    /// Order is NORMATIVE (issue #221): per variant, in index order —
    /// (1) reduce; (2) [`append_variant_label`] on every series
    /// (overriding any common-pipeline `__variant__`); (3) that variant's
    /// (already `__variant__`-injected) vector aggregations — SKIPPED
    /// when it carries none (`apply_vector_aggs` would round-trip every
    /// matrix series' points through a `BTreeMap` for an identity
    /// transform); (4) concatenate in index order into one pre-sized
    /// output; (5) **range only**: drop every series whose label set has
    /// no `__variant__` (reference `engine.go:485-487`); instant keeps
    /// them (`engine.go:620-634`) — the reference's instant/range
    /// asymmetry.
    fn finish_in_place(&mut self, warnings: &mut Warnings) -> Result<QueryResult, ReadError> {
        let is_range = matches!(
            self.variants.first().map(plan::VariantSpec::window),
            Some(ClientWindow::Range { .. })
        );
        let subs = std::mem::take(&mut self.subs);
        let mut per_variant: Vec<QueryResult> = Vec::with_capacity(subs.len());
        for (i, sub) in subs.into_iter().enumerate() {
            let spec = &self.variants[i];
            let mut out = sub.finish()?;
            // Adjudicated correction (issue #221, capture-governed): the
            // reference attaches `__variant__` per EXTRACTED SAMPLE (the
            // consolidated extractor), so an `absent_over_time` variant's
            // SYNTHETIC series never carries it — index-less (and kept)
            // at instant, dropped by the range filter below, grouped as
            // `{}` under an outer aggregation. Container-captured
            // (`b13_variants.test` absent cases); the approved plan's
            // append-to-every-series order was wrong here.
            if !matches!(spec.client().range_op, RangeAggOp::AbsentOverTime) {
                match &mut out {
                    QueryResult::Vector(items) => {
                        for s in items.iter_mut() {
                            append_variant_label(&mut s.labels, spec.index());
                        }
                    }
                    QueryResult::Matrix(items) => {
                        for s in items.iter_mut() {
                            append_variant_label(&mut s.labels, spec.index());
                        }
                    }
                    _ => {}
                }
            }
            let out = if spec.vector_aggs().is_empty() {
                out
            } else {
                apply_vector_aggs(out, spec.vector_aggs())?
            };
            // Issue #236: the result-series cap is applied PER VARIANT,
            // before the concat — matching the reference's own granularity
            // (`engine.go:474-506`, `:609-621` apply `maxSeries` per
            // variant, not to the concatenated whole). Strictly more
            // permissive than capping the concat: a 3-variant query
            // returning 400 series each is served (1 200 result series).
            //
            // Issue #277: a breach SKIPS the variant and records a
            // warning; it is not an error. The reference removes the
            // whole variant — never truncates it — and serves the rest at
            // 200 (`pkg/logql/engine.go:500-508 @ grafana/loki v3.7.4
            // b318f2829f0ae2094ab3a1e90780450e9e4b03be`).
            //
            // THE DISCHARGE RUNS ON BOTH ARMS, and above the decision.
            // Before #277 the `?` on the gate returned before it, which
            // was invisible only because the error propagated past
            // `finish`'s `debug_assert_eq!(self.charged, self.base)`.
            // Now that a breach continues, leaving it below would fire
            // that assert on the first skip in any debug build.
            let breach = result_series_breach(&out);
            discharge_fanout_bytes(&mut self.charged, self.sub_charged[i]);
            match breach {
                Some(cap) => warnings.add(variant_series_warning(cap, spec.index())),
                None => per_variant.push(out),
            }
        }
        if is_range {
            let total: usize = per_variant
                .iter()
                .map(|r| match r {
                    QueryResult::Matrix(items) => items.len(),
                    _ => 0,
                })
                .sum();
            let mut out: Vec<MatrixSeries> = Vec::with_capacity(total);
            for r in per_variant {
                if let QueryResult::Matrix(items) = r {
                    out.extend(
                        items
                            .into_iter()
                            .filter(|s| s.labels.iter().any(|(k, _)| k == VARIANT_LABEL)),
                    );
                }
            }
            Ok(QueryResult::Matrix(out))
        } else {
            let total: usize = per_variant
                .iter()
                .map(|r| match r {
                    QueryResult::Vector(items) => items.len(),
                    _ => 0,
                })
                .sum();
            let mut out: Vec<VectorSample> = Vec::with_capacity(total);
            for r in per_variant {
                if let QueryResult::Vector(items) = r {
                    out.extend(items);
                }
            }
            Ok(QueryResult::Vector(out))
        }
    }

    /// Finalizes, asserting every per-sub-state charge was released.
    /// `warnings` collects one message per SKIPPED variant (issue #277).
    pub fn finish(mut self, warnings: &mut Warnings) -> Result<QueryResult, ReadError> {
        let out = self.finish_in_place(warnings)?;
        debug_assert_eq!(
            self.charged, self.base,
            "every variants fan-out charge must be discharged at finish"
        );
        let _ = self.cap;
        let _ = self.arena;
        Ok(out)
    }
}

/// Sets `__variant__` to the plain decimal `index`, OVERRIDING any
/// existing `__variant__` the common pipeline produced (the reference
/// appends a DUPLICATE label there and then mis-routes samples by
/// re-parsing the two-valued set — provably wrong output, deliberately
/// not reproduced; ledgered), keeping the vector key-sorted. Writes the
/// value `String` ONCE — `set_label_sorted`'s shape with the `String`
/// moved into whichever arm takes it (that helper allocates
/// `value.to_string()` in both arms and is deliberately left untouched).
pub fn append_variant_label(labels: &mut Vec<(String, String)>, index: usize) {
    let value = index.to_string();
    match labels.binary_search_by(|(k, _)| k.as_str().cmp(VARIANT_LABEL)) {
        Ok(i) => labels[i].1 = value,
        Err(i) => labels.insert(i, (VARIANT_LABEL.to_string(), value)),
    }
}

/// The pure, slice-driven variants evaluation — the twin of
/// [`run_client_agg_rows`] the hermetic corpus runner drives, through the
/// SAME [`VariantArena`] + [`VariantsAggState`], so the corpus exercises
/// the identical charging path. Checks order ONCE and sorts ONCE into at
/// most one local `Vec` pushed into every sub-state — it never delegates
/// to `run_client_agg_rows`, whose per-sub-state re-sort would clone
/// every scanned body N times (issue #221 member Δ6.2.4).
pub fn run_variants_rows(
    rows: &[MetricScanRow],
    meta: &HashMap<u64, StreamMetaRow>,
    common: &[Stage],
    variants: &[plan::VariantSpec],
    warnings: &mut Warnings,
) -> Result<QueryResult, ReadError> {
    let arena = VariantArena::build(common, variants, MAX_VARIANT_FANOUT_STATE_BYTES, 0)?;
    let mut state = VariantsAggState::new(&arena, variants, meta, MAX_VARIANT_FANOUT_STATE_BYTES)?;
    let is_range = matches!(
        variants.first().map(plan::VariantSpec::window),
        Some(ClientWindow::Range { .. })
    );
    if is_range {
        let ordered = rows.windows(2).all(|w| {
            (w[0].fingerprint, w[0].timestamp_ns) <= (w[1].fingerprint, w[1].timestamp_ns)
        });
        if ordered {
            state.push_rows(rows)?;
        } else {
            let mut sorted: Vec<MetricScanRow> = rows.to_vec();
            sorted.sort_by(|a, b| {
                a.fingerprint
                    .cmp(&b.fingerprint)
                    .then(a.timestamp_ns.cmp(&b.timestamp_ns))
            });
            state.push_rows(&sorted)?;
        }
    } else {
        state.push_rows(rows)?;
    }
    state.finish(warnings)
}

#[cfg(test)]
mod tests {
    use super::super::labels::series_labels;
    use super::super::params::{Direction, PlanCtx, QueryParams, QuerySpec};
    use super::super::plan::{MetricNode, MetricPlan, Plan};
    use super::*;
    use crate::logql::testkit::*;

    // -----------------------------------------------------------------
    // Issue #221: variants fan-out — caps, charges, arena, guards.
    // -----------------------------------------------------------------

    fn variants_ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    fn variants_range_spec() -> QuerySpec {
        QuerySpec::Range {
            start_ns: 0,
            end_ns: 60 * VSEC,
            step_ns: 60 * VSEC as u64 as i64 as u64,
        }
    }

    /// Plans a variants query and returns `(scan, specs, spec_bytes)`.
    fn variants_fixture(query: &str, spec: QuerySpec) -> (MetricPlan, Vec<plan::VariantSpec>, u64) {
        let expr = pulsus_logql::parse(query).expect("parse");
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        // Issue #272: E0509 — re-bind through a reference and take the
        // owned pieces out of the borrow.
        let mut planned = plan::plan(&expr, &params, &variants_ctx()).expect("plan");
        match &mut planned {
            Plan::MetricBinary(MetricNode::Variants {
                scan,
                variants,
                spec_bytes,
            }) => ((**scan).clone(), std::mem::take(variants), *spec_bytes),
            other => panic!("expected a variants plan, got {other:?}"),
        }
    }

    /// `variants(<v>, <v>, …xN) of (<common>)`.
    fn n_variant_query(n: usize, variant: &str, common: &str) -> String {
        let list = vec![variant; n].join(", ");
        format!("variants({list}) of ({common})")
    }

    /// I1 — CHARGE: the driver-buffer term. Pair: N = 64 vs 65 tail-free
    /// variants (single axis: N). Deleting the
    /// `variant_driver_buffer_bytes` charge makes the measured delta 0
    /// while the expected formula stays > 0.
    #[test]
    fn i1_driver_buffer_term_is_charged() {
        let common = r#"{app="x"}[5m]"#;
        let arena_charged = |n: usize| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(n, r#"count_over_time({app="x"}[5m])"#, common),
                variants_range_spec(),
            );
            let cp = scan.client.expect("client scan");
            VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0)
                .expect("build")
                .charged_bytes()
        };
        let expected = variant_driver_buffer_bytes(65) - variant_driver_buffer_bytes(64);
        assert!(expected > 0);
        assert_eq!(arena_charged(65) - arena_charged(64), expected);
    }

    /// I2 — CHARGE: the arena-entry term. Pair: variant 1's tail
    /// identical to variant 0's vs distinct with EQUAL source bytes
    /// (single axis: tail identity). Deleting the per-entry charge before
    /// `extended_with` zeroes the measured delta.
    #[test]
    fn i2_arena_entry_term_is_charged() {
        let charged = |second: &str| {
            let q = format!(
                r#"variants(sum_over_time({{app="x"}} | unwrap aa [5m]), sum_over_time({{app="x"}} | unwrap {second} [5m])) of ({{app="x"}} | logfmt [5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let tail1 = variants[1].client().pipeline.clone();
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            (arena.charged_bytes(), arena.len(), cp.pipeline, tail1)
        };
        let (same, same_len, _, _) = charged("aa");
        let (distinct, distinct_len, common, tail_bb) = charged("bb");
        assert_eq!(same_len, 2, "identical tails share one entry");
        assert_eq!(distinct_len, 3, "distinct tails add one entry each");
        let expected = variant_pipeline_entry_bytes(&common, &tail_bb);
        assert!(expected > 0);
        assert_eq!(distinct - same, expected);
    }

    /// I3 — CHARGE: the boxed sub-state slot. Pair: N = 2 vs 3, empty
    /// meta, `count_over_time`, no absent labels (single axis: N; every
    /// other `variant_state_bytes` term is 0).
    #[test]
    fn i3_sub_state_slot_term_is_charged() {
        let sub_charges = |n: usize| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(n, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                variants_range_spec(),
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let expected = alloc_block_bytes(size_of::<RangeSlideState<'_>>() as u64);
        assert!(expected > 0);
        assert_eq!(sub_charges(3) - sub_charges(2), expected);
    }

    fn k_stream_meta(k: u64) -> HashMap<u64, StreamMetaRow> {
        (0..k)
            .map(|i| {
                (
                    i + 1,
                    StreamMetaRow {
                        fingerprint: i + 1,
                        service: format!("svc{i}"),
                        labels: format!(r#"{{"env":"prod","idx":"{i}"}}"#),
                    },
                )
            })
            .collect()
    }

    /// The I4/I5 expected meta term, built INDEPENDENTLY of
    /// `variant_meta_snapshot_bytes` (same inputs, formula spelled out) so
    /// deleting the runtime charge fails the equality.
    fn expected_meta_term(meta: &HashMap<u64, StreamMetaRow>) -> u64 {
        let mut bytes = 0u64;
        for m in meta.values() {
            let labels = series_labels(m);
            bytes += map_entry_bytes(size_of::<(u64, LabelSet)>()) + label_set_bytes(&labels);
        }
        // Issue #344: unconditional — both sub-state kinds hold `hashes`.
        bytes += meta.len() as u64 * map_entry_bytes(size_of::<(u64, u64)>());
        bytes
    }

    /// Issue #260 AC 4, runtime leg (review round 1, C6: the lexical
    /// `attach_fold` census cannot see a qualified call, an alias or a
    /// future dispatch arrangement, so it is a tripwire and not the
    /// proof).
    ///
    /// A REALLY CONSTRUCTED variants state, for a RANGE query, reports
    /// zero folds across its sub-states — which is what keeps
    /// [`super::super::charge::LEAF_COUNTERS`]`.group_bytes` at 2 rather
    /// than 3.
    ///
    /// Two independent reasons, both asserted rather than argued:
    ///
    /// 1. the fan-out never attaches one (`sub_folded_aggs() == 0` on a
    ///    state built through the production constructor), and
    /// 2. the only thing a leaf ever folds — an OUTER vector aggregation
    ///    — cannot syntactically wrap a variants query at all
    ///    (`sum by (…) (variants(…))` is the reference-exact 400, issue
    ///    #221), so there is nothing to attach even if a caller wanted
    ///    to.
    ///
    /// Non-vacuous by construction: the last leg attaches a fold to a
    /// plain `RangeSlideState` and shows it reports 1, so the zero above
    /// is the variants path declining a mechanism that works, not the
    /// mechanism being absent.
    #[test]
    fn the_variants_sub_states_take_no_fold() {
        let query = n_variant_query(3, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#);
        let (scan, variants, _) = variants_fixture(&query, variants_range_spec());
        let cp = scan.client.expect("client scan");
        let meta = k_stream_meta(2);
        let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
        let st = VariantsAggState::new(&arena, &variants, &meta, u64::MAX).expect("state");
        assert_eq!(st.subs.len(), 3, "one sub-state per variant");
        assert_eq!(
            st.sub_folded_aggs(),
            0,
            "a variants sub-state took a fold — that is a THIRD live group-byte counter, and \
             MAX_LEAF_RETAINED_BYTES prices two"
        );

        // (2) An outer vector aggregation cannot reach a variants
        // sub-state: the composition is rejected before planning.
        assert!(
            pulsus_logql::parse(&format!("sum by (app) ({query})")).is_err(),
            "an outer vector aggregation over variants must stay a 400 — if it became legal, \
             something would have to decide whether its fold reaches the sub-states"
        );

        // (3) The mechanism the sub-states decline is live and reachable.
        let client = plan::ClientAgg {
            pipeline: cp.pipeline.clone(),
            value: plan::ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
        let mut plain = RangeSlideState::new(
            &compiled,
            &meta,
            &client,
            slide_window(0, 100, 10, 30),
            None,
            AggCaps::DEFAULT,
        )
        .expect("plain range leaf");
        assert_eq!(plain.folded_aggs(), 0);
        plain.attach_fold(&(
            pulsus_logql::VectorAggOp::Sum,
            Some(pulsus_logql::Grouping {
                kind: pulsus_logql::GroupingKind::By,
                labels: vec!["app".to_string()],
            }),
            None,
        ));
        assert_eq!(
            plain.folded_aggs(),
            1,
            "the fold mechanism must be live, or the zero above proves nothing"
        );
    }

    /// I4 — CHARGE: the `base_labels` half of the meta snapshot. Pair:
    /// INSTANT kind (state type and constructor fixed), meta K = 4 vs 0.
    #[test]
    fn i4_meta_base_labels_term_is_charged() {
        let sub_charges = |meta: &HashMap<u64, StreamMetaRow>| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                QuerySpec::Instant { at_ns: 60 * VSEC },
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st = VariantsAggState::new(&arena, &variants, meta, u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let meta = k_stream_meta(4);
        let expected = expected_meta_term(&meta);
        assert!(expected > 0);
        assert_eq!(sub_charges(&meta) - sub_charges(&HashMap::new()), expected);
    }

    /// I5 — CHARGE: the `hashes` TABLE share, for **both** sub-state
    /// kinds.
    ///
    /// **Re-aimed by issue #344, and the re-aim is the point.** It used
    /// to isolate the term by DIFFERENCE: only the sliding kind held a
    /// `hashes` map, so charging the range fixture and not the instant
    /// one made the term visible and let I5 fail alone. The instant kind
    /// now holds the map too (`first_over_time`/`last_over_time` take the
    /// endpoints of Loki's `(timestamp, stream_hash, tie_rank)` order on
    /// both paths), so that difference is structurally zero and the old
    /// assertion had degenerated into `expected > expected`, which is
    /// false for every input — it could only ever have passed while the
    /// two kinds disagreed.
    ///
    /// What replaces it guards the failure that actually happened here:
    /// ONE kind gaining state the charge still prices for the other. Both
    /// fixtures are charged, both must equal the same independently
    /// spelled expression, and the `hashes` half is asserted to be a
    /// nonzero component of it — so deleting the term from
    /// `variant_meta_snapshot_bytes`, or reintroducing a per-kind
    /// condition on it, fails here.
    #[test]
    fn i5_meta_hashes_table_share_is_charged_for_both_sub_state_kinds() {
        let sub_charges = |spec: QuerySpec, meta: &HashMap<u64, StreamMetaRow>| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                spec,
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st = VariantsAggState::new(&arena, &variants, meta, u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let meta = k_stream_meta(4);
        let expected = expected_meta_term(&meta);
        // The `hashes` half, spelled on its own: a real, nonzero part of
        // the expression above, so the equalities below cannot be
        // satisfied by a charge that omits it.
        let hashes_half = meta.len() as u64 * map_entry_bytes(size_of::<(u64, u64)>());
        assert!(hashes_half > 0 && expected > hashes_half);
        for spec in [
            QuerySpec::Instant { at_ns: 60 * VSEC },
            variants_range_spec(),
        ] {
            assert_eq!(
                sub_charges(spec, &meta) - sub_charges(spec, &HashMap::new()),
                expected,
                "both sub-state kinds hold `base_labels` AND `hashes`, so both must be                  charged for both: {spec:?}"
            );
        }
    }

    /// I6 — CHARGE: the range kind's construction-time `absent_labels`
    /// clone. Pair: same absent op, same (empty) meta, same grid; the
    /// variant's own selector carries 3 Eq matchers vs 1.
    #[test]
    fn i6_absent_labels_term_is_charged() {
        let sub_charges = |selector: &str| {
            let q = format!(
                r#"variants(absent_over_time({selector}[5m]), absent_over_time({selector}[5m])) of ({{app="x"}}[5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let three = sub_charges(r#"{a="1", b="2", c="3"}"#);
        let one = sub_charges(r#"{a="1"}"#);
        let labels3: LabelSet = vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
            ("c".into(), "3".into()),
        ];
        let labels1: LabelSet = vec![("a".into(), "1".into())];
        let expected = label_set_bytes(&labels3) - label_set_bytes(&labels1);
        assert!(expected > 0);
        assert_eq!(three - one, expected);
    }

    /// I7 — CHARGE: the `present_cover` term. Pair: same range grid, the
    /// variant selector carries NO Eq matcher (absent labels empty in
    /// both), op `absent_over_time` vs `count_over_time` — the single
    /// moving term is the grid array.
    #[test]
    fn i7_present_cover_term_is_charged() {
        let sub_charges = |op: &str| {
            let q = format!(
                r#"variants({op}({{app=~"x.*"}}[5m]), {op}({{app=~"x.*"}}[5m])) of ({{app="x"}}[5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        // Grid 0..60s step 60s ⇒ points {0, 60} ⇒ kmax = 1 ⇒ 8·(kmax+2).
        let expected = alloc_block_bytes(8 * 3);
        assert_eq!(
            sub_charges("absent_over_time") - sub_charges("count_over_time"),
            expected
        );
    }

    /// B10 / AC 17 — the arena shares, never recompiles: tail-free
    /// variants all share entry 0 (no entry charge — at N = 1 the whole
    /// arena charge is exactly 0, AC 14); identical tails share an entry;
    /// distinct tails add one each (pinned by I2's lengths too).
    #[test]
    fn arena_dedups_on_the_tail_slice_alone() {
        let arena_of = |q: &str| {
            let (scan, variants, _) = variants_fixture(q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let n = variants.len() as u64;
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            (arena.len(), arena.charged_bytes(), n)
        };
        let (len, charged, _) =
            arena_of(r#"variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m])"#);
        assert_eq!((len, charged), (1, 0), "AC 14: a 1-variant arena charges 0");
        let (len, charged, n) = arena_of(&n_variant_query(
            3,
            r#"count_over_time({app="x"}[5m])"#,
            r#"{app="x"}[5m]"#,
        ));
        assert_eq!(len, 1, "tail-free variants all share entry 0");
        assert_eq!(
            charged,
            variant_driver_buffer_bytes(n),
            "no entry term for shared tails"
        );
    }

    /// B6 — charge-before-allocate at the state boundary: a cap that
    /// admits the arena but not the first extra sub-state trips
    /// `VariantStateBytes` BEFORE that sub-state is constructed.
    #[test]
    fn b6_tiny_cap_trips_before_the_extra_sub_state_exists() {
        let (scan, variants, _) = variants_fixture(
            &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
            variants_range_spec(),
        );
        let cp = scan.client.expect("client scan");
        let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
        // Cap == the arena's own charge: sub-state 0 is free, sub-state 1's
        // slot charge breaches.
        let err = VariantsAggState::new(&arena, &variants, &HashMap::new(), arena.charged_bytes())
            .expect_err("the extra sub-state must breach");
        match err {
            ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { bytes, cap }) => {
                assert!(bytes > cap);
            }
            other => panic!("expected VariantStateBytes, got {other:?}"),
        }
        // And the driver-buffer charge itself trips first under a
        // near-zero cap (B6b) — before any reservation.
        let err = VariantArena::build(&cp.pipeline, &variants, 1, 0)
            .expect_err("the driver buffers must breach");
        assert!(matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { .. })
        ));
    }

    /// AC 24 + the D5.5 field-addition guard: exhaustive destructuring
    /// (no `..`), every container exactly reserved. Adding a field to
    /// either driver struct breaks this test's compilation, forcing the
    /// W-MEM walk to be re-run for it. Buckets: pipelines/slot/subs/
    /// sub_charged (C, charged via `variant_driver_buffer_bytes`),
    /// charged/base/cap (S), arena/variants (B).
    #[test]
    fn every_driver_container_field_is_accounted() {
        let (scan, variants, _) = variants_fixture(
            &n_variant_query(3, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
            variants_range_spec(),
        );
        let cp = scan.client.expect("client scan");
        let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
        {
            let mut st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.push_rows(&[]).expect("empty push");
            let VariantsAggState {
                arena: _b_arena,     // B
                variants: _b_specs,  // B
                subs,                // C — with_capacity(n)
                sub_charged,         // C — with_capacity(n)
                charged: _s_charged, // S
                base: _s_base,       // S
                cap: _s_cap,         // S
            } = st;
            assert_eq!(subs.capacity(), 3);
            assert_eq!(sub_charged.capacity(), 3);
        }
        let n = variants.len() as u64;
        let VariantArena {
            pipelines,        // C — with_capacity(n + 1)
            slot,             // C — with_capacity(n)
            charged: a_bytes, // S
        } = arena;
        assert_eq!(pipelines.capacity(), 4);
        assert_eq!(slot.capacity(), 3);
        assert!(a_bytes >= variant_driver_buffer_bytes(n));
    }

    /// AC 51 census — `extended_with(` has exactly ONE production call
    /// site (the arena), and `VariantArena::build`'s body materializes no
    /// `common ++ tail` (no `.concat(`/`.to_vec()`/`chain(`): the dedup
    /// key is the tail slice alone. Production text = everything before
    /// the column-0 `#[cfg(test)]`, `//` comment text stripped (the
    /// `search_sql.rs` census precedent).
    #[test]
    fn variants_exec_census() {
        let src = include_str!("variants.rs");
        let production = src
            .split("\n#[cfg(test)]")
            .next()
            .expect("split")
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            production.matches("extended_with(").count(),
            1,
            "exactly one extended_with call (the arena)"
        );
        let build_body = {
            let start = production.find("    pub fn build(").expect("build fn");
            let tail = &production[start..];
            let end = tail.find("\n    }").expect("build end");
            &tail[..end]
        };
        for forbidden in [".concat(", ".to_vec()", "chain("] {
            assert!(
                !build_body.contains(forbidden),
                "VariantArena::build must not materialize common ++ tail ({forbidden})"
            );
        }
        // The exec-side charge funnel has exactly 3 call sites: the
        // driver buffers, the arena entry, the sub-state. Counted over a
        // WHITESPACE-STRIPPED copy so rustfmt line wrapping cannot move
        // the census; the raw token also matches
        // `discharge_fanout_bytes(&mut` as a substring, so the release
        // sites are subtracted.
        let compact: String = production.chars().filter(|c| !c.is_whitespace()).collect();
        let charges = compact.matches("charge_fanout_bytes(&mut").count()
            - compact.matches("discharge_fanout_bytes(&mut").count();
        assert_eq!(charges, 3, "exec charge-site census");
    }

    // -----------------------------------------------------------------
    // Issue #277: the per-variant cap SKIPS and WARNS.
    // -----------------------------------------------------------------

    /// The b21 corpus shape, in miniature: `n` distinct `| logfmt`
    /// groups on one stream, variant 0 = one series per group (so it is
    /// the variant that walks the cap), variant 1 = exactly one series.
    fn cap_query(n: usize, range: bool) -> String {
        let common = format!(r#"{{app="v277"}} | logfmt | id < {n} [1m]"#);
        let _ = range;
        format!("variants(sum by (id) (count_over_time({common})), sum(count_over_time({common}))) of ({common})")
    }

    fn cap_rows(n: usize) -> Vec<MetricScanRow> {
        (0..n)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                // 100 ms apart inside the (0, 60s] window, like b21.
                timestamp_ns: (i as i64 + 1) * 100 * 1_000_000,
                body: format!("id={i}"),
                structured_metadata: String::new(),
            })
            .collect()
    }

    fn cap_meta() -> HashMap<u64, StreamMetaRow> {
        let mut m = HashMap::new();
        m.insert(
            1,
            StreamMetaRow {
                fingerprint: 1,
                service: "v277".to_string(),
                labels: r#"{"app":"v277"}"#.to_string(),
            },
        );
        m
    }

    /// Runs the cap fixture over `n` groups and returns the result plus
    /// the accumulated warnings, in wire order.
    fn run_cap(n: usize, spec: QuerySpec) -> (QueryResult, Vec<String>) {
        let range = matches!(spec, QuerySpec::Range { .. });
        let (scan, variants, _) = variants_fixture(&cap_query(n, range), spec);
        let common = scan.client.expect("client scan").pipeline;
        let mut warnings = Warnings::new();
        let out = run_variants_rows(&cap_rows(n), &cap_meta(), &common, &variants, &mut warnings)
            .expect("a breaching variant is skipped, never an error");
        (out, warnings.as_strings())
    }

    fn variant_indices(result: &QueryResult) -> Vec<String> {
        let pick = |labels: &Vec<(String, String)>| {
            labels
                .iter()
                .find(|(k, _)| k == VARIANT_LABEL)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| "<none>".to_string())
        };
        let mut out: Vec<String> = match result {
            QueryResult::Vector(items) => items.iter().map(|s| pick(&s.labels)).collect(),
            QueryResult::Matrix(items) => items.iter().map(|s| pick(&s.labels)).collect(),
            other => panic!("expected a series result, got {other:?}"),
        };
        out.sort();
        out
    }

    /// AC 2 — engine-level skip and message. The breaching variant
    /// contributes ZERO series (removed entirely, never truncated), the
    /// call returns `Ok`, and the message is byte-equal to the string
    /// captured from the pinned container.
    #[test]
    fn variants_breaching_variant_is_skipped_with_a_warning() {
        let (out, warnings) = run_cap(501, QuerySpec::Instant { at_ns: 60 * VSEC });

        assert_eq!(
            warnings,
            vec!["maximum of series (500) reached for variant (0)".to_string()],
            "the captured message, byte for byte"
        );
        let indices = variant_indices(&out);
        assert!(
            !indices.iter().any(|i| i == "0"),
            "the breaching variant contributes zero series, got {indices:?}"
        );
        assert_eq!(
            indices,
            vec!["1".to_string()],
            "the surviving variant is served in full"
        );
    }

    /// AC 2's companion boundary, both sides: exactly the cap is SERVED
    /// with no warning, and `cap + 1` skips. Captured on the pinned
    /// container at 498/499/500 served and 501 skipped
    /// (`b21_variant_series_cap.test` cases 1-4); this pins the two rows
    /// either side of the edge at the unit level, where the corpus pins
    /// all four end to end.
    #[test]
    fn the_per_variant_boundary_serves_exactly_the_cap_and_skips_one_more() {
        let (at_cap, none) = run_cap(500, QuerySpec::Instant { at_ns: 60 * VSEC });
        assert!(none.is_empty(), "exactly the cap emits no warning");
        // 500 + 1 = 501 RESULT series across two variants, served: the
        // cap is PER VARIANT, never on the concatenation.
        match &at_cap {
            QueryResult::Vector(items) => assert_eq!(items.len(), 501),
            other => panic!("expected a vector, got {other:?}"),
        }

        let (_, over) = run_cap(501, QuerySpec::Instant { at_ns: 60 * VSEC });
        assert_eq!(over.len(), 1, "one more than the cap skips");
    }

    /// AC 14 — **a deliberate over-acceptance, defended.** A RANGE
    /// variants query whose variant yields exactly 500 series is served
    /// with no warning. The pinned reference SKIPS it there:
    /// `multiVariantVectorsToSeries` (`pkg/logql/engine.go:473-508 @
    /// grafana/loki v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`)
    /// tests `len(sm[variantLabel]) >= maxSeries` **before** looking up
    /// whether the incoming point's series already exists, so on the
    /// second grid point a point belonging to an already-counted series
    /// deletes the whole variant. Its own sibling
    /// `vectorsToSeriesWithLimit` (`:440-471`) has exactly the `!ok`
    /// guard this function is missing, and the reference's own INSTANT
    /// path serves 500 — the two disagree with each other.
    ///
    /// PulsusDB applies `> 500` uniformly, which is a strict
    /// over-acceptance: everything the reference serves, PulsusDB
    /// serves. Adjudicated on issue #277 and recorded as ledger entry
    /// `(d)` in `docs/benchmarks/logs-differential-ledger.md`.
    ///
    /// **Changing our gate to `>= 500` to "match the reference" reddens
    /// this test.** That is the whole point of it.
    #[test]
    fn range_variant_at_the_cap_is_served_where_the_reference_skips_it() {
        let (out, warnings) = run_cap(
            500,
            QuerySpec::Range {
                start_ns: 60 * VSEC,
                end_ns: 120 * VSEC,
                step_ns: 30 * VSEC as u64,
            },
        );
        assert!(
            warnings.is_empty(),
            "PulsusDB serves the range case the reference drops; got {warnings:?}"
        );
        let indices = variant_indices(&out);
        assert_eq!(
            indices.iter().filter(|i| *i == "0").count(),
            500,
            "the 500-series variant survives in full"
        );
        assert_eq!(indices.iter().filter(|i| *i == "1").count(), 1);
    }

    /// AC 9 — **the charge invariant survives a skip.** A breaching
    /// variant's fan-out bytes were charged and must be discharged.
    /// Before issue #277 the gate's `?` returned before
    /// `discharge_fanout_bytes`, which was invisible only because the
    /// error propagated past `finish`'s
    /// `debug_assert_eq!(self.charged, self.base)`. Now that a breach
    /// CONTINUES, that assert is reachable: moving the discharge back
    /// below the gate panics this test in any debug build.
    ///
    /// Driven through `VariantsAggState` directly rather than through
    /// [`run_variants_rows`], so the post-condition is observed at
    /// `finish` with the charge counter still inspectable beforehand.
    #[test]
    fn a_skipped_variant_still_discharges_its_fanout_charge() {
        let (scan, variants, spec_bytes) = variants_fixture(
            &cap_query(501, false),
            QuerySpec::Instant { at_ns: 60 * VSEC },
        );
        let common = scan.client.expect("client scan").pipeline;
        let arena = VariantArena::build(
            &common,
            &variants,
            MAX_VARIANT_FANOUT_STATE_BYTES,
            spec_bytes,
        )
        .expect("arena");
        let meta = cap_meta();
        let mut st = VariantsAggState::new(
            &arena,
            &variants,
            &meta,
            MAX_VARIANT_FANOUT_STATE_BYTES,
        )
        .expect("state");
        st.push_rows(&cap_rows(501)).expect("push");
        let base = st.base;
        assert!(
            st.charged > base,
            "the fan-out sub-states must be charged before finish"
        );

        let mut warnings = Warnings::new();
        // `finish` consumes the state and asserts `charged == base`. In a
        // debug build the assert is live, so reaching `Ok` here IS the
        // proof; the explicit skip assertion below stops the test from
        // passing because nothing breached.
        let out = st.finish(&mut warnings).expect("a skip is not an error");
        assert_eq!(warnings.len(), 1, "exactly one variant was skipped");
        assert!(!variant_indices(&out).iter().any(|i| i == "0"));
    }

    /// The retention ceiling, at the seam that produces it: a query whose
    /// variants ALL breach accumulates exactly one message per variant
    /// and no more, whatever the row volume. The pure-value form of the
    /// same property is
    /// `warnings_retention_is_bounded_by_the_variant_count`
    /// (`src/logql/warnings.rs`).
    #[test]
    fn every_breaching_variant_contributes_exactly_one_warning() {
        let common = r#"{app="v277"} | logfmt | id < 501 [1m]"#;
        let query = format!(
            "variants(count_over_time({common}), bytes_over_time({common})) of ({common})"
        );
        let (scan, variants, _) =
            variants_fixture(&query, QuerySpec::Instant { at_ns: 60 * VSEC });
        let common_pipeline = scan.client.expect("client scan").pipeline;
        let mut warnings = Warnings::new();
        let out = run_variants_rows(
            &cap_rows(501),
            &cap_meta(),
            &common_pipeline,
            &variants,
            &mut warnings,
        )
        .expect("every variant skipping is still a success");

        assert_eq!(
            warnings.as_strings(),
            vec![
                "maximum of series (500) reached for variant (0)".to_string(),
                "maximum of series (500) reached for variant (1)".to_string(),
            ]
        );
        assert!(warnings.len() <= variants.len());
        match out {
            QueryResult::Vector(items) => assert!(items.is_empty(), "every variant was removed"),
            other => panic!("expected an empty vector, got {other:?}"),
        }
    }
}
