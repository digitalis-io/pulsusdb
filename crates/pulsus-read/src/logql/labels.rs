//! The label/JSON codec: rendering a label set to the wire's canonical
//! JSON, parsing it back, and the stable hashes keyed off it.
//!
//! [`render_labels_json_sorted`] and [`render_series_labels`] are the
//! only writers of the labels JSON a response carries;
//! [`parse_flat_labels`] and [`parse_flat_labels_into`] are the only
//! readers. [`fnv1a64`] and [`stream_hash`] are the stable hashes, and
//! [`StructuredMetadataCtx`] carries the reserved structured-metadata
//! routing [`merge_labels_with_structured_metadata`] performs.

use super::pipeline::{ERROR_DETAILS_LABEL, ERROR_LABEL};
use super::rows::StreamMetaRow;
use std::borrow::Cow;

/// Renders a SORTED label set as the oracle's series shape
/// (`{a="b", c="d"}`) for the surviving-`__error__` query failure.
/// Values are escaped with the same mandatory-set escaper the canonical
/// labels JSON uses ([`push_json_string`] — quotes, backslashes, and
/// control characters; the shape the reference renders with Go-style
/// quoting), so hostile parsed label values can never produce malformed
/// `{k="v"}` text (review round 1, finding 4).
pub(in crate::logql) fn render_series_labels(sorted: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in sorted.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        push_json_string(&mut out, v);
    }
    out.push('}');
    out
}

/// Loki `labels.StableHash` (v3.7.4 `model/labels/sharding.go:24`): the
/// same-timestamp cross-stream tie key. `xxhash64` (seed 0) over each label
/// **in sorted-by-name order** rendered `name·0xFF·value·0xFF` (`sep =
/// '\xff'`, `labels_common.go:38`). Build-tag-independent — byte-identical
/// to Loki. `sorted_labels` is the stream's base label set (as
/// [`series_labels`] renders it: canonical labels + the `service_name`
/// column), already sorted by name.
pub(in crate::logql) fn stream_hash(sorted_labels: &[(String, String)]) -> u64 {
    let mut buf: Vec<u8> = Vec::new();
    for (name, value) in sorted_labels {
        buf.extend_from_slice(name.as_bytes());
        buf.push(0xFF);
        buf.extend_from_slice(value.as_bytes());
        buf.push(0xFF);
    }
    xxhash_rust::xxh64::xxh64(&buf, 0)
}

/// Renders a **sorted** label set to the canonical flat-label JSON shape
/// (`{"key":"value",...}`, sorted keys, no nesting — docs/architecture.md
/// §2.3), matching what the writer produces for base streams so the
/// server encoder can splice it verbatim either way. Hand-rolled
/// escaping (byte-compatible with `serde_json`'s string escaping —
/// unit-tested below) so rendering borrows the label pairs instead of
/// cloning them into a `serde_json::Map` (round-2 finding 1).
pub(in crate::logql) fn render_labels_json_sorted(
    sorted_labels: &[(Cow<'_, str>, Cow<'_, str>)],
) -> String {
    let mut out = String::with_capacity(
        2 + sorted_labels
            .iter()
            .map(|(k, v)| k.len() + v.len() + 6)
            .sum::<usize>(),
    );
    out.push('{');
    for (i, (k, v)) in sorted_labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_string(&mut out, k);
        out.push(':');
        push_json_string(&mut out, v);
    }
    out.push('}');
    out
}

/// Appends `s` as a quoted JSON string, escaping exactly the mandatory
/// set the same way `serde_json` does (`"`/`\` escaped, the five short
/// control escapes, `\u00xx` lowercase for the rest of C0, everything
/// else verbatim).
pub(in crate::logql) fn push_json_string(out: &mut String, s: &str) {
    use std::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                // Infallible: `write!` to a String cannot fail.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// FNV-1a 64 — the fan-out path's deterministic label-set fingerprint
/// (`fingerprint = hash(final labels)`, plan v1). Not a stored/write-path
/// fingerprint: purely a stable response identity for derived streams.
pub(in crate::logql) fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A stream's full exposed label set: its canonical-JSON labels plus the
/// promoted `service` physical column re-injected as `service_name`
/// (docs/architecture.md §2.3's canonical label model) so grouping by
/// `service_name` — the §3.2 canonical vector-agg example — works without
/// special-casing it against the JSON blob.
pub(in crate::logql) fn series_labels(meta: &StreamMetaRow) -> Vec<(String, String)> {
    let mut labels = parse_flat_labels(&meta.labels);
    labels.retain(|(k, _)| k != "service_name");
    labels.push(("service_name".to_string(), meta.service.clone()));
    labels.sort();
    labels
}

/// Parses PulsusDB's canonical flat label JSON (`{"key":"value", ...}`,
/// sorted keys, no nesting — docs/architecture.md §2.3) without a JSON
/// crate dependency (not part of this module's declared dependency set).
/// Malformed input — which should never occur, this only ever reads back
/// what the writer produced — yields whatever pairs were parsed so far
/// rather than panicking.
pub(in crate::logql) fn parse_flat_labels(json: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    parse_flat_labels_into(json, &mut out);
    out
}

/// [`parse_flat_labels`] that APPENDS into a caller-owned buffer instead of
/// allocating a fresh `Vec` (issue #97): the structured-metadata merge reuses
/// one buffer across rows (clear + refill), so the parse must not allocate its
/// own return vector per row.
pub(in crate::logql) fn parse_flat_labels_into(json: &str, out: &mut Vec<(String, String)>) {
    let mut chars = json.chars().peekable();
    while let Some(&c) = chars.peek() {
        chars.next();
        if c == '{' {
            break;
        }
    }
    loop {
        skip_ws(&mut chars);
        match chars.peek() {
            None | Some('}') => break,
            Some(',') => {
                chars.next();
                continue;
            }
            Some('"') => {}
            Some(_) => break,
        }
        let Some(key) = parse_json_string(&mut chars) else {
            break;
        };
        skip_ws(&mut chars);
        if chars.peek() == Some(&':') {
            chars.next();
        }
        skip_ws(&mut chars);
        let Some(value) = parse_json_string(&mut chars) else {
            break;
        };
        out.push((key, value));
    }
}

/// `LabelsBuilder.Add`'s routing of ONE row's structured metadata
/// (`pkg/logql/log/labels.go:392-412`; issue #238): the base-collision
/// `_extracted` suffix (`parser.go:25`) is applied FIRST (`labels.go:395-397`);
/// a name that is STILL exactly `__error__`/`__error_details__` is assigned to
/// the OUT-OF-BAND slot and `return`s (`labels.go:399-407`) — it never reaches
/// `Set`, so it does NOT dirty the builder. EMPTY VALUES: `Add` assigns them
/// verbatim, and an empty slot is unset (`HasErr` = `err != ""`), so an empty
/// reserved SM value contributes nothing at all. Live-probed at v3.7.4 across
/// eleven SM shapes (`discover_log_levels: false` — with it on, every stream
/// carries a `detected_level` SM entry and the clean-builder fast paths are
/// unreachable).
#[derive(Debug, Default, Clone)]
pub struct StructuredMetadataCtx {
    /// SM `__error__` (post-suffix), routed to the err slot. "" == absent.
    pub err: String,
    /// SM `__error_details__` (post-suffix), routed to the details slot.
    /// "" == absent.
    pub details: String,
    /// At least one NON-EMPTY ordinary SM entry reached `Set` — the
    /// reference's `hasAdd()`. Empty-valued entries are excluded because the
    /// reference's distributor strips them before they can reach the builder
    /// (`pkg/distributor/distributor.go:698-723` through Prometheus'
    /// `labels.Builder`, which deletes empty-valued base labels —
    /// `labels_slicelabels.go:404-412`); PulsusDB stores them (#259), and
    /// counting only non-empty entries here keeps the details-visibility
    /// gate reference-exact regardless of what ingest stored (live-probed).
    pub has_ordinary: bool,
}

/// The shared no-structured-metadata context. NOT a `const`: `String` has a
/// `Drop` impl, so `&SOME_CONST` would be a temporary and could not satisfy
/// a `&'a` parameter; a `static` gives a `&'static` that coerces to any `'a`.
pub static EMPTY_STRUCTURED_METADATA: StructuredMetadataCtx = StructuredMetadataCtx {
    err: String::new(),
    details: String::new(),
    has_ordinary: false,
};

impl StructuredMetadataCtx {
    /// No reserved entry and no non-empty ordinary entry — the row
    /// contributes nothing to the error slots or the dirty bit.
    pub fn is_empty(&self) -> bool {
        self.err.is_empty() && self.details.is_empty() && !self.has_ordinary
    }

    /// The gated view for the NO-PIPELINE fast path (`fan_out_sm_fast_path`,
    /// which runs no `CompiledPipeline` and must therefore apply the
    /// materialisation gate itself — issue #238): `__error__` iff non-empty;
    /// `__error_details__` iff non-empty AND (`has_ordinary` OR `err`
    /// non-empty) — the same `visible()` rule the pipeline's `ErrorSlots`
    /// applies at emit. Upserts into the already-merged label vector
    /// (`set_label` semantics — the slot value wins on a same-name entry).
    pub fn append_visible(&self, out: &mut Vec<(String, String)>) {
        let upsert = |out: &mut Vec<(String, String)>, key: &str, value: &str| match out
            .iter_mut()
            .find(|(k, _)| k == key)
        {
            Some(slot) => slot.1 = value.to_string(),
            None => out.push((key.to_string(), value.to_string())),
        };
        if !self.err.is_empty() {
            upsert(out, ERROR_LABEL, &self.err);
        }
        if !self.details.is_empty() && (self.has_ordinary || !self.err.is_empty()) {
            upsert(out, ERROR_DETAILS_LABEL, &self.details);
        }
    }
}

/// Merges a stream's cached base (stream/parsed) labels with one row's
/// structured metadata into `merge_buf` (cleared first — its heap allocation
/// is reused across rows; `sm_buf` is a second reused scratch the SM pairs are
/// parsed into). A structured-metadata key that collides with a base label key
/// is renamed to `<key>_extracted`; the resolved key is then UPSERTED into the
/// merged set (last-write-wins) so BOTH the base label and the renamed SM value
/// survive as distinct entries in the ordinary case. This matches
/// grafana/loki:3.4.2's DEFAULT query response (probed for issue #97): the
/// stream/parsed label keeps the original key and value, while the colliding
/// structured-metadata value surfaces under the `_extracted` suffix (and is
/// filterable there — `| key_extracted="v"` matches, `| key="v"` matches the
/// stream label).
///
/// DOUBLE collision: when the renamed `<key>_extracted` ALSO already exists —
/// e.g. base carries both `env` AND `env_extracted`, or the SM object itself
/// supplies `env_extracted` alongside a colliding `env` — the upsert OVERWRITES
/// that existing slot rather than emitting a second `<key>_extracted` entry.
/// grafana/loki:3.4.2 renders exactly one `env_extracted`, last-write-wins
/// (probed for issue #97: base `env`+`env_extracted` + SM `env` → the SM value
/// wins the `env_extracted` slot; no `env_extracted_extracted`, no numeric
/// suffix, no drop). This is the same collision precedence the `| json`
/// parser's `add_extracted` already pins, and it preserves the
/// no-duplicate-label-entries invariant under ANY collision pattern. The rename
/// decision consults only the base region; the upsert consults the FULL evolving
/// merged set (base + already-merged SM keys). The result is left UNSORTED;
/// callers sort before rendering/grouping.
///
/// **Reserved-name routing (issue #238):** a pair whose POST-suffix key is
/// exactly `__error__`/`__error_details__` goes into `sm_ctx` (the reference's
/// `Add` routes it to the out-of-band slot and returns — `labels.go:399-407`)
/// and is NEVER pushed into `merge_buf`; every other pair upserts as before
/// and, when non-empty, sets `sm_ctx.has_ordinary`. `sm_ctx` is cleared and
/// refilled per row. The suffix rule runs FIRST, so a base `__error__` +
/// SM `__error__` yields an ordinary `__error___extracted` entry (probed).
pub(in crate::logql) fn merge_labels_with_structured_metadata(
    base: &[(String, String)],
    structured_metadata: &str,
    merge_buf: &mut Vec<(String, String)>,
    sm_buf: &mut Vec<(String, String)>,
    sm_ctx: &mut StructuredMetadataCtx,
) {
    sm_ctx.err.clear();
    sm_ctx.details.clear();
    sm_ctx.has_ordinary = false;
    merge_buf.clear();
    merge_buf.extend(base.iter().cloned());
    let base_len = merge_buf.len();
    sm_buf.clear();
    parse_flat_labels_into(structured_metadata, sm_buf);
    // `base_len` is small (a stream's label count), so these scans are bounded
    // by the fixed label cardinality, not by row count. `drain` moves the owned
    // key/value Strings out of the reused scratch without cloning.
    for (mut key, value) in sm_buf.drain(..) {
        if merge_buf[..base_len].iter().any(|(bk, _)| *bk == key) {
            key.push_str("_extracted");
        }
        if key == ERROR_LABEL {
            // Assign, INCLUDING an empty value (an empty slot is unset).
            sm_ctx.err = value;
            continue;
        }
        if key == ERROR_DETAILS_LABEL {
            sm_ctx.details = value;
            continue;
        }
        sm_ctx.has_ordinary |= !value.is_empty();
        match merge_buf.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => merge_buf.push((key, value)),
        }
    }
}

fn skip_ws<I: Iterator<Item = char>>(chars: &mut std::iter::Peekable<I>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
}

fn parse_json_string<I: Iterator<Item = char>>(
    chars: &mut std::iter::Peekable<I>,
) -> Option<String> {
    if chars.next() != Some('"') {
        return None;
    }
    let mut out = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'u' => {
                    let hex: String = (0..4).filter_map(|_| chars.next()).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(code)
                    {
                        out.push(c);
                    }
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::testkit::*;

    /// AC2: `stream_hash` == Loki's `labels.StableHash`, pinned against
    /// GOLDEN VALUES CAPTURED FROM THE REFERENCE ITSELF — computed by calling
    /// `labels.StableHash` in the pinned `grafana/loki` v3.7.4 checkout
    /// (`vendor/github.com/prometheus/prometheus/model/labels/sharding.go`,
    /// default build tags, the shape Loki release builds use). These are
    /// external constants, NOT a recomputation of our own implementation, so
    /// a change to our byte layout/seed reddens this test.
    #[test]
    fn stream_hash_matches_loki_stable_hash_golden_values() {
        let lbl = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        // (sorted labels, Loki v3.7.4 `labels.StableHash` value)
        let cases: &[(Vec<(String, String)>, u64)] = &[
            (
                lbl(&[("app", "a"), ("env", "prod")]),
                8_934_535_624_278_967_805,
            ),
            (
                lbl(&[("env", "prod"), ("service_name", "checkout")]),
                3_591_138_641_183_557_463,
            ),
            (lbl(&[]), 17_241_709_254_077_376_921),
            (lbl(&[("k", "v")]), 3_592_197_247_305_585_030),
            (
                lbl(&[("__name__", "x"), ("job", "j"), ("zz", "last")]),
                12_310_789_843_392_592_049,
            ),
        ];
        for (labels, want) in cases {
            assert_eq!(
                stream_hash(labels),
                *want,
                "StableHash mismatch for {labels:?}"
            );
        }
    }

    /// Round-2 finding 1: the hand-rolled borrowed-label JSON renderer
    /// must stay byte-compatible with `serde_json`'s escaping (the shape
    /// the writer/encoder ecosystem produces and splices verbatim).
    #[test]
    fn render_labels_json_sorted_matches_serde_json_escaping_byte_for_byte() {
        let pairs = [
            ("plain", "value"),
            ("quote", r#"a"b"#),
            ("backslash", r"a\b"),
            ("newline_tab", "a\nb\tc"),
            ("carriage_bs_ff", "a\rb\u{08}c\u{0C}d"),
            ("low_control", "a\u{01}b\u{1f}c"),
            ("unicode", "日本語µ"),
        ];
        let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = pairs
            .iter()
            .map(|(k, v)| (Cow::Borrowed(*k), Cow::Borrowed(*v)))
            .collect();
        let ours = render_labels_json_sorted(&sorted);
        // serde_json reference rendering of the same ordered pairs.
        let mut reference = String::from("{");
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                reference.push(',');
            }
            reference.push_str(&serde_json::to_string(k).unwrap());
            reference.push(':');
            reference.push_str(&serde_json::to_string(v).unwrap());
        }
        reference.push('}');
        assert_eq!(ours, reference);
        // And the canonical shape stays round-trippable / re-parseable.
        let parsed = parse_flat_labels(&ours);
        assert_eq!(parsed.len(), pairs.len());
    }

    #[test]
    fn fnv1a64_is_stable_and_content_sensitive() {
        let a = fnv1a64(br#"{"a":"1"}"#);
        assert_eq!(a, fnv1a64(br#"{"a":"1"}"#));
        assert_ne!(a, fnv1a64(br#"{"a":"2"}"#));
    }

    #[test]
    fn parse_flat_labels_reads_simple_pairs() {
        let pairs = parse_flat_labels(r#"{"env":"prod","team":"checkout"}"#);
        assert_eq!(
            pairs,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "checkout".to_string())
            ]
        );
    }

    #[test]
    fn parse_flat_labels_handles_escaped_quotes_and_backslashes() {
        let pairs = parse_flat_labels(r#"{"msg":"a\"b\\c"}"#);
        assert_eq!(pairs, vec![("msg".to_string(), "a\"b\\c".to_string())]);
    }

    #[test]
    fn parse_flat_labels_of_empty_object_is_empty() {
        assert!(parse_flat_labels("{}").is_empty());
    }

    #[test]
    fn series_labels_injects_service_name_from_the_physical_column() {
        let meta = StreamMetaRow {
            fingerprint: 1,
            service: "checkout".to_string(),
            labels: r#"{"env":"prod"}"#.to_string(),
        };
        let labels = series_labels(&meta);
        assert!(labels.contains(&("service_name".to_string(), "checkout".to_string())));
        assert!(labels.contains(&("env".to_string(), "prod".to_string())));
    }

    /// Runs one row's SM through the real merge/routing; returns the SORTED
    /// merged (ordinary) set and the routing outcome.
    fn route_sm(
        base: &[(&str, &str)],
        sm_json: &str,
    ) -> (Vec<(String, String)>, StructuredMetadataCtx) {
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
        merge_buf.sort();
        (merge_buf, ctx)
    }

    /// D2/D3 (kills W1, W8): reserved SM names route to the ctx — never
    /// into the merged set, never counting as ordinary.
    #[test]
    fn reserved_sm_names_route_to_the_ctx_not_the_merged_set() {
        // v2: reserved err only.
        let (merged, ctx) = route_sm(&[("service_name", "v2")], r#"{"__error__":"boom"}"#);
        assert_eq!(merged, owned_pairs(&[("service_name", "v2")]));
        assert_eq!(ctx.err, "boom");
        assert_eq!(ctx.details, "");
        assert!(!ctx.has_ordinary);
        // v3: reserved details only.
        let (merged, ctx) = route_sm(&[("service_name", "v3")], r#"{"__error_details__":"bdet"}"#);
        assert_eq!(merged, owned_pairs(&[("service_name", "v3")]));
        assert_eq!(ctx.details, "bdet");
        assert!(!ctx.has_ordinary && ctx.err.is_empty());
        // v9: mixed reserved err + ordinary.
        let (merged, ctx) = route_sm(
            &[("service_name", "v9")],
            r#"{"__error__":"boom","trace_id":"abc"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "v9"), ("trace_id", "abc")])
        );
        assert_eq!(ctx.err, "boom");
        assert!(ctx.has_ordinary);
        // v1: ordinary only.
        let (merged, ctx) = route_sm(&[("service_name", "v1")], r#"{"trace_id":"abc"}"#);
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "v1"), ("trace_id", "abc")])
        );
        assert!(ctx.has_ordinary && ctx.err.is_empty() && ctx.details.is_empty());
    }

    /// D5 (kills W2 at the routing seam): EMPTY reserved values are
    /// assigned verbatim — and an empty slot is unset (`is_empty`), so the
    /// v4/v5/v12 shapes contribute nothing at all.
    #[test]
    fn empty_reserved_sm_values_leave_the_ctx_empty() {
        for (id, sm_json) in [
            ("v4", r#"{"__error__":""}"#),
            ("v5", r#"{"__error_details__":""}"#),
            ("v12", r#"{"__error__":"","__error_details__":""}"#),
        ] {
            let (merged, ctx) = route_sm(&[("service_name", id)], sm_json);
            assert_eq!(merged, owned_pairs(&[("service_name", id)]), "{id}");
            assert!(ctx.is_empty(), "{id}: {ctx:?}");
        }
    }

    /// D4 (kills W7): `has_ordinary` counts NON-EMPTY ordinary entries only
    /// — the reference's distributor strips empty-valued SM before it can
    /// reach the builder (#259 tracks PulsusDB's ingest divergence; the
    /// stray `trace_id=""` in the merged set is that issue, not this one).
    #[test]
    fn empty_ordinary_sm_values_do_not_set_has_ordinary() {
        // w1.
        let (merged, ctx) = route_sm(&[("service_name", "w1")], r#"{"trace_id":""}"#);
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "w1"), ("trace_id", "")])
        );
        assert!(!ctx.has_ordinary);
        // w2: an empty ordinary + a reserved details.
        let (merged, ctx) = route_sm(
            &[("service_name", "w2")],
            r#"{"trace_id":"","__error_details__":"bdet"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "w2"), ("trace_id", "")])
        );
        assert_eq!(ctx.details, "bdet");
        assert!(!ctx.has_ordinary);
    }

    /// D1 (kills W3a — C7's routing seam): the `_extracted` base-collision
    /// suffix runs BEFORE the reserved-name test, so a base `__error__` +
    /// SM `__error__` yields an ORDINARY `__error___extracted` entry and an
    /// empty ctx.
    #[test]
    fn the_extracted_suffix_preempts_the_reserved_err_check() {
        let (merged, ctx) = route_sm(
            &[("__error__", "streamerr"), ("service_name", "v10")],
            r#"{"__error__":"boom"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[
                ("__error__", "streamerr"),
                ("__error___extracted", "boom"),
                ("service_name", "v10"),
            ])
        );
        assert!(ctx.err.is_empty());
        assert!(ctx.has_ordinary);
    }

    /// D1 (kills W3b — C25/C26's routing seam): same for the details branch
    /// (`v13`, the round-4 test gap).
    #[test]
    fn the_extracted_suffix_preempts_the_reserved_details_check() {
        let (merged, ctx) = route_sm(
            &[("__error_details__", "streamdet"), ("service_name", "v13")],
            r#"{"__error_details__":"smdet"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
            ])
        );
        assert!(ctx.details.is_empty());
        assert!(ctx.has_ordinary);
    }
}
