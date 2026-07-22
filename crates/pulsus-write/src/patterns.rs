//! Deterministic, stateless log-pattern extraction (M7-C3, issue #171).
//!
//! [`extract_template`] is a pure function `body -> template`: the same line
//! always yields the same template — everywhere, on every shard/replica, and
//! across retries. Identity is the template String itself, so counts sum
//! correctly under `log_patterns`'s `AggregatingMergeTree(sum)` merges (a
//! drain-style online clusterer keeps order-dependent per-stream mutable state
//! that would emit *different* templates for identical lines and break both the
//! mergeable aggregation and idempotent re-inserts — rejected in the design
//! spike). The trade-off is coarser clustering (a digit-free variable word
//! stays literal), revisitable later as a read-time secondary merge without
//! touching stored rows.
//!
//! Normative rules (D1, frozen by the golden tests below):
//!  1. Only the first [`PATTERN_SCAN_PREFIX_BYTES`] bytes of `body` are
//!     examined (cut on a UTF-8 char boundary); a longer body yields a template
//!     ending in `<_>` (the partial trailing token is dropped).
//!  2. Tokens are maximal runs of non-ASCII-whitespace; the template joins
//!     tokens with single spaces (whitespace runs collapse — deterministic;
//!     documented as "normalized, not round-trip matchable").
//!  3. Per token: leading/trailing wrapper punctuation [`WRAPPER_PUNCT`] stays
//!     literal; if the core splits as `key=value` or `key:value` (both halves
//!     nonempty, at the first separator), `key` + separator stay literal and
//!     only the value fragment is classified.
//!  4. A fragment becomes `<_>` iff it contains an ASCII digit OR exceeds
//!     [`PATTERN_MAX_FRAGMENT_BYTES`] bytes. No placeholder collapsing.
//!  5. Caps: at most [`PATTERN_MAX_TOKENS`] tokens and at most
//!     [`PATTERN_MAX_TEMPLATE_BYTES`] template bytes; truncation is at a token
//!     boundary plus a trailing `<_>`. Empty/whitespace-only body ⇒ no row.

use std::collections::HashMap;

use crate::protocols::otlp_logs::LogRow;
use crate::writer::LogPatternRow;

/// Only the first 1 KiB of a log body is examined (D1 rule 1). A documented
/// constant, not a config knob (the #115 ingest-cap precedent).
pub const PATTERN_SCAN_PREFIX_BYTES: usize = 1024;
/// At most 64 tokens contribute to a template (D1 rule 5).
pub const PATTERN_MAX_TOKENS: usize = 64;
/// A template is at most 512 bytes, trailing `<_>` included (D1 rule 5).
pub const PATTERN_MAX_TEMPLATE_BYTES: usize = 512;
/// A fragment longer than this classifies to `<_>` even without a digit
/// (D1 rule 4).
pub const PATTERN_MAX_FRAGMENT_BYTES: usize = 64;
/// The fixed ingest bucket, 10s in nanoseconds (D2). Deliberately NOT the
/// `{{log_rollup_suffix}}`-named LogQL resolution — patterns need no step
/// alignment, and a fixed constant avoids the config-named identity of id 9.
pub const PATTERN_BUCKET_NS: i64 = 10_000_000_000;

/// Fixed floor of the per-request aggregation structures regardless of entry
/// count (D3/v4): hashbrown's minimum non-empty table (~4 buckets × 49 B
/// slot+ctrl + SIMD group tail ≈ 212 B measured floor), the
/// `HashMap`/`RandomState` struct, the output `Vec` header, and allocator
/// size-class rounding — with ~4.8× margin. Charged once per batch.
pub const AGG_BASE_OVERHEAD: u64 = 1024;
/// Marginal per-distinct-entry ceiling once the base is absorbed (v3/v4): the
/// worst mid-rehash transient is 24/7 slots/entry × 49 B ≈ 168 B plus the
/// 48 B exact-capacity output-row slot = 216 B, rounded up.
pub const PATTERN_ROW_OVERHEAD: u64 = 256;
/// Hard cap on distinct templates per request batch (v4). Once at cap, a row
/// whose template is not already present is dropped from pattern accounting
/// only (the log line itself is untouched) and counted via the writer's
/// `patterns_dropped_total` — making the aggregation buffer a FIXED ceiling
/// (`AGG_BASE_OVERHEAD + CAP × PATTERN_ROW_OVERHEAD ≈ 2.44 MiB/request`),
/// independent of any hashbrown modeling.
pub const MAX_DISTINCT_PATTERNS_PER_BATCH: usize = 10_000;

/// The placeholder a classified variable fragment renders to.
const PLACEHOLDER: &str = "<_>";
/// Wrapper punctuation stripped (kept literal) from a token's ends (D1 rule 3).
const WRAPPER_PUNCT: &[char] = &['(', ')', '{', '}', '[', ']', '"', '\'', ',', ';'];

/// The result of aggregating one request batch's log rows into `log_patterns`
/// rows. `dropped` counts rows whose (unseen) template was refused at the
/// [`MAX_DISTINCT_PATTERNS_PER_BATCH`] cap — folded into the writer's
/// `patterns_dropped_total`; the log lines themselves are unaffected.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PatternAggregation {
    pub rows: Vec<LogPatternRow>,
    pub dropped: u64,
}

/// Extracts the deterministic template for `body`, or `None` for an
/// empty/whitespace-only body (D1). Pure: same body ⇒ same template.
///
/// **Bounded scratch (issue #171 review finding 1):** tokens are rendered and
/// appended one at a time into the single output `String`, with the
/// [`PATTERN_MAX_TOKENS`] cap and the [`PATTERN_MAX_TEMPLATE_BYTES`] cap both
/// applied *during* the stream — never collecting all tokens into an
/// intermediate `Vec<String>` first. So a pathological body (~512 one-byte
/// tokens in 1 KiB) allocates only the ≤ 512-byte output plus one transient
/// rendered token, NOT ~512 heap Strings — the transient peak the
/// reserve-before-materialize charge (`est_template_bound` + the per-batch
/// base) actually covers.
pub fn extract_template(body: &str) -> Option<String> {
    // Rule 1: examine only the first PATTERN_SCAN_PREFIX_BYTES bytes, cut on a
    // UTF-8 char boundary. A longer body drops its (possibly partial) trailing
    // token and forces a trailing `<_>`.
    let (prefix, prefix_truncated, boundary_mid_token) = if body.len() > PATTERN_SCAN_PREFIX_BYTES {
        let mut cut = PATTERN_SCAN_PREFIX_BYTES;
        while cut > 0 && !body.is_char_boundary(cut) {
            cut -= 1;
        }
        // The final examined token straddles the cut (is truncated mid-token)
        // unless the cut lands on a token boundary — i.e. the prefix ends with
        // whitespace, or the first dropped byte is whitespace. A char-boundary
        // reduction always leaves a non-whitespace multibyte lead byte at
        // `cut`, so a straddled multibyte char reads as mid-token (dropped).
        let mid_token = !prefix_ends_at_boundary(body, cut);
        (&body[..cut], true, mid_token)
    } else {
        (body, false, false)
    };

    // A body that exceeded the prefix always ends the template in `<_>` (the
    // dropped tail); a mid-token cut additionally drops the partial final
    // token below.
    let mut truncated = prefix_truncated;
    let mut out = String::new();
    let mut token_count = 0usize;

    // Rule 2: tokens are maximal runs of non-ASCII-whitespace. Streamed via a
    // peekable iterator so the mid-token partial final token can be dropped
    // without materializing the full token list.
    let mut tokens = prefix.split_ascii_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if boundary_mid_token && tokens.peek().is_none() {
            // The partial final token is dropped (`truncated` already set).
            break;
        }
        if token_count >= PATTERN_MAX_TOKENS {
            truncated = true;
            break;
        }
        // One transient rendered token, dropped at the end of this iteration —
        // never accumulated into a Vec. `overflowed` = the token's own render
        // exceeds the template cap, so no token boundary can ever include it
        // (D1 rule 5): drop it here and mark the truncation.
        let (rendered, overflowed) = render_token(tok);
        if overflowed {
            truncated = true;
            break;
        }
        let sep = usize::from(!out.is_empty());
        if out.len() + sep + rendered.len() > PATTERN_MAX_TEMPLATE_BYTES {
            // The next whole token does not fit — stop at the last complete
            // token (rule 5: truncation is at a token boundary).
            truncated = true;
            break;
        }
        if sep == 1 {
            out.push(' ');
        }
        out.push_str(&rendered);
        token_count += 1;
    }

    if truncated {
        // Rule 5: make room for the trailing " <_>" (or "<_>" when empty)
        // within the 512-byte cap by dropping WHOLE trailing tokens. Rendered
        // tokens never contain a space, so each space in `out` is a token
        // boundary — `rfind(' ')` pops one token cleanly, no offset bookkeeping.
        while out.len() + usize::from(!out.is_empty()) + PLACEHOLDER.len()
            > PATTERN_MAX_TEMPLATE_BYTES
        {
            match out.rfind(' ') {
                Some(pos) => out.truncate(pos),
                None => out.clear(),
            }
        }
        if out.is_empty() {
            out.push_str(PLACEHOLDER);
        } else {
            out.push(' ');
            out.push_str(PLACEHOLDER);
        }
    }

    if out.is_empty() { None } else { Some(out) }
}

/// `true` when the examined prefix `body[..cut]` ends at a clean token
/// boundary: either the last examined byte is ASCII whitespace, or the first
/// dropped byte (`body[cut]`) is. Used to decide whether the final examined
/// token was cut mid-token (D1 rule 1). `cut < body.len()` by construction
/// (this is only called when the body exceeded the prefix).
fn prefix_ends_at_boundary(body: &str, cut: usize) -> bool {
    let bytes = body.as_bytes();
    let last_examined_ws = cut == 0 || bytes[cut - 1].is_ascii_whitespace();
    let first_dropped_ws = bytes[cut].is_ascii_whitespace();
    last_examined_ws || first_dropped_ws
}

/// Renders one token: `leading_wrapper + classified_core + trailing_wrapper`,
/// where the core is either `key + sep + classify(value)` (a `key=value` /
/// `key:value` split) or `classify(core)` (D1 rules 3–4). Returns
/// `(rendered, overflowed)`.
///
/// **Whole-token truncation (D1 rule 5: "truncation is at a token boundary"):**
/// a token whose FULL natural render would exceed [`PATTERN_MAX_TEMPLATE_BYTES`]
/// can never fit any template (which is ≤ 512), so there is no valid
/// token-boundary that includes it — this returns `overflowed = true` (and an
/// empty string, so nothing large is even built) and the caller drops it at the
/// boundary and emits the trailing `<_>`. A token that fits is built in FULL
/// into a single buffer (no partial fragment is ever emitted mid-token, and no
/// mid-fragment byte-slice is taken — so char-boundary safety is structural).
/// The natural length is summed from fragment lengths WITHOUT allocating, so the
/// only heap string built is the ≤ 512-byte in-bounds render.
fn render_token(token: &str) -> (String, bool) {
    let leading = &token[..token.len() - token.trim_start_matches(WRAPPER_PUNCT).len()];
    let after_leading = &token[leading.len()..];
    let core = after_leading.trim_end_matches(WRAPPER_PUNCT);
    let trailing = &after_leading[core.len()..];

    match split_key_value(core) {
        Some((key, sep, value)) => {
            let value = classify(value);
            let natural = leading.len() + key.len() + sep.len_utf8() + value.len() + trailing.len();
            if natural > PATTERN_MAX_TEMPLATE_BYTES {
                return (String::new(), true);
            }
            let mut out = String::with_capacity(natural);
            out.push_str(leading);
            out.push_str(key);
            out.push(sep);
            out.push_str(value);
            out.push_str(trailing);
            (out, false)
        }
        None => {
            let core = classify(core);
            let natural = leading.len() + core.len() + trailing.len();
            if natural > PATTERN_MAX_TEMPLATE_BYTES {
                return (String::new(), true);
            }
            let mut out = String::with_capacity(natural);
            out.push_str(leading);
            out.push_str(core);
            out.push_str(trailing);
            (out, false)
        }
    }
}

/// Splits `core` at the first `=` or `:` separator into `(key, sep, value)`,
/// requiring both halves nonempty (D1 rule 3). `None` otherwise.
fn split_key_value(core: &str) -> Option<(&str, char, &str)> {
    let idx = core.find(['=', ':'])?;
    let sep = core[idx..].chars().next()?;
    let key = &core[..idx];
    let value = &core[idx + sep.len_utf8()..];
    if key.is_empty() || value.is_empty() {
        return None;
    }
    Some((key, sep, value))
}

/// Classifies a fragment: `<_>` iff it contains an ASCII digit OR exceeds
/// [`PATTERN_MAX_FRAGMENT_BYTES`] bytes; otherwise the fragment verbatim
/// (D1 rule 4). No placeholder collapsing.
fn classify(fragment: &str) -> &str {
    if fragment.len() > PATTERN_MAX_FRAGMENT_BYTES || fragment.bytes().any(|b| b.is_ascii_digit()) {
        PLACEHOLDER
    } else {
        fragment
    }
}

/// A pre-extraction upper bound on the template heap bytes a row's body can
/// produce (D1/D3): `min(2 × examined + 8, 512)`, where `examined =
/// min(body.len(), 1024)`. Never under-estimates the eventual template length
/// (the 2× amplification proof: a 1-byte variable token `"1"` renders `"<_>"`,
/// worst-case 2× per token; literal tokens never grow) — so the writer's
/// reserve-before-materialize charge always covers the real pattern String.
pub fn est_template_bound(row: &LogRow) -> u64 {
    let examined = row.body.len().min(PATTERN_SCAN_PREFIX_BYTES) as u64;
    (2 * examined + 8).min(PATTERN_MAX_TEMPLATE_BYTES as u64)
}

/// Aggregates one request batch's rows into `(fingerprint, bucket_ns, pattern)
/// -> count` `log_patterns` rows (D2). Runs only after the reservation
/// succeeds (`writer::LogWriter::admit_batch`). Empty-body rows produce no
/// template. At the [`MAX_DISTINCT_PATTERNS_PER_BATCH`] cap, a row with an
/// unseen template is dropped from pattern accounting only (counted in
/// `dropped`), in the batch's deterministic parse order.
pub fn aggregate_patterns(rows: &[LogRow]) -> PatternAggregation {
    let mut map: HashMap<(u64, i64, String), u64> = HashMap::new();
    let mut dropped = 0u64;

    for row in rows {
        let Some(template) = extract_template(&row.body) else {
            continue;
        };
        let bucket_ns = row.timestamp_ns.0.div_euclid(PATTERN_BUCKET_NS) * PATTERN_BUCKET_NS;
        let key = (row.fingerprint, bucket_ns, template);
        if let Some(count) = map.get_mut(&key) {
            *count += 1;
        } else if map.len() < MAX_DISTINCT_PATTERNS_PER_BATCH {
            map.insert(key, 1);
        } else {
            // At cap: refuse the unseen template (its transient String is freed
            // here, under this row's own template-bound charge). Log line
            // untouched.
            dropped += 1;
        }
    }

    // Preallocated EXACTLY at map.len() (v3 derivation — the per-entry overhead
    // constant is sized for growth-based map sizing, not a rows-wide table).
    let mut out: Vec<LogPatternRow> = Vec::with_capacity(map.len());
    for ((fingerprint, bucket_ns, pattern), count) in map {
        out.push(LogPatternRow {
            fingerprint,
            bucket_ns,
            pattern,
            count,
        });
    }
    // Deterministic output order (the merge engine is order-agnostic; this
    // keeps fidelity/golden assertions stable). Sorting is in place — no
    // reallocation past the exact-capacity Vec above.
    out.sort_by(|a, b| {
        (a.fingerprint, a.bucket_ns, &a.pattern).cmp(&(b.fingerprint, b.bucket_ns, &b.pattern))
    });

    PatternAggregation { rows: out, dropped }
}

#[cfg(test)]
mod tests {
    use pulsus_model::UnixNano;

    use super::*;

    fn log_row(fingerprint: u64, ts_ns: i64, body: &str) -> LogRow {
        LogRow {
            service: "svc".to_string(),
            fingerprint,
            timestamp_ns: UnixNano(ts_ns),
            severity: 0,
            body: body.to_string(),
            structured_metadata: String::new(),
        }
    }

    // -- D1 rule 4: digit/length classification --------------------------

    #[test]
    fn a_digit_bearing_token_classifies_to_placeholder() {
        assert_eq!(extract_template("user 12 login").unwrap(), "user <_> login");
    }

    #[test]
    fn a_digit_free_word_stays_literal_the_accepted_coarseness() {
        // The deliberate trade-off vs drain: a variable digit-free word is not
        // clustered away.
        assert_eq!(
            extract_template("connection refused abruptly").unwrap(),
            "connection refused abruptly"
        );
    }

    #[test]
    fn a_fragment_over_the_byte_cap_classifies_even_without_a_digit() {
        let long = "a".repeat(PATTERN_MAX_FRAGMENT_BYTES + 1);
        assert_eq!(extract_template(&long).unwrap(), "<_>");
        let ok = "a".repeat(PATTERN_MAX_FRAGMENT_BYTES);
        assert_eq!(extract_template(&ok).unwrap(), ok);
    }

    // -- D1 rule 2: whitespace runs collapse -----------------------------

    #[test]
    fn whitespace_runs_collapse_to_single_spaces() {
        assert_eq!(
            extract_template("a\t\t b\n  c").unwrap(),
            "a b c",
            "tabs/newlines/multi-space runs are all token separators"
        );
    }

    // -- D1 rule 3: wrapper punctuation + key=value/key:value ------------

    #[test]
    fn wrapper_punctuation_stays_literal_around_a_classified_core() {
        assert_eq!(extract_template("(12)").unwrap(), "(<_>)");
        assert_eq!(extract_template("[abc],").unwrap(), "[abc],");
    }

    #[test]
    fn key_equals_value_keeps_key_and_separator_classifying_only_the_value() {
        assert_eq!(extract_template("id=42").unwrap(), "id=<_>");
        assert_eq!(extract_template("status:200").unwrap(), "status:<_>");
        // Digit-free value stays literal.
        assert_eq!(extract_template("level=info").unwrap(), "level=info");
    }

    #[test]
    fn key_value_uses_the_first_separator_only() {
        // First `=` splits; the remainder is the value fragment, classified
        // whole (contains a digit).
        assert_eq!(extract_template("a=b=3").unwrap(), "a=<_>");
        // A `:` before an `=` is the first separator.
        assert_eq!(extract_template("k:v=1").unwrap(), "k:<_>");
    }

    #[test]
    fn key_value_with_an_empty_half_is_not_a_split() {
        // Leading `=` — key half empty ⇒ classify the whole core.
        assert_eq!(extract_template("=5").unwrap(), "<_>");
        // Trailing `:` — value half empty ⇒ classify the whole core (no digit,
        // stays literal).
        assert_eq!(extract_template("key:").unwrap(), "key:");
    }

    #[test]
    fn wrapper_punctuation_is_stripped_before_the_key_value_split() {
        assert_eq!(extract_template("(id=42)").unwrap(), "(id=<_>)");
    }

    /// D1 rule 3 fidelity (issue #171 round-3): a 257–512 byte digit-free
    /// `key=value` token — larger than the retired 256-byte per-token cap but
    /// within the 512-byte template cap — keeps its LITERAL key (only the value
    /// is classified). Collapsing it to `<_>` early would violate D1 rule 3 for
    /// a token that fits a template; the per-token bound is now the template cap
    /// so this renders per the rules.
    #[test]
    fn a_long_digit_free_key_value_token_within_the_template_cap_keeps_its_literal_key() {
        // A 300-byte key (well past 256, under 512) + a short digit-free value.
        let key = "k".repeat(300);
        assert!(key.len() > 256 && key.len() < PATTERN_MAX_TEMPLATE_BYTES);
        let template = extract_template(&format!("{key}=info")).unwrap();
        // Literal key + separator preserved; digit-free value stays literal too.
        assert_eq!(template, format!("{key}=info"));
        assert!(template.len() <= PATTERN_MAX_TEMPLATE_BYTES);
        // And the value half is still subject to the 64-byte classify cap.
        let digit_value = extract_template(&format!("{key}=99")).unwrap();
        assert_eq!(digit_value, format!("{key}=<_>"));
    }

    /// D1 rule 5 case (a) — a SINGLE token whose full natural render alone
    /// exceeds the 512-byte template cap (a >512 byte literal key) can fit no
    /// token boundary, so the template is just `<_>` — NEVER a bare partial key
    /// (the round-5 finding: mid-fragment budget truncation used to emit the
    /// partial key with no trailing marker).
    #[test]
    fn rule5_a_single_oversized_key_value_token_is_just_the_placeholder() {
        let key = "k".repeat(PATTERN_MAX_TEMPLATE_BYTES + 10);
        assert_eq!(extract_template(&format!("{key}=info")).unwrap(), "<_>");
    }

    /// D1 rule 5 case (b) — an oversized FIRST token followed by more tokens:
    /// truncation stops at that token boundary (nothing precedes it), so the
    /// template is `<_>` and the trailing `end` is not appended (the codex
    /// "huge_kv + end" trace).
    #[test]
    fn rule5_b_oversized_first_token_followed_by_more_is_the_placeholder() {
        let key = "k".repeat(PATTERN_MAX_TEMPLATE_BYTES + 10);
        assert_eq!(extract_template(&format!("{key}=info end")).unwrap(), "<_>");
    }

    /// D1 rule 5 case (b′) — an oversized token PRECEDED by a fitting token
    /// keeps the last complete token plus the trailing marker (`small <_>`),
    /// never a partial oversized token.
    #[test]
    fn rule5_b_oversized_token_after_a_fitting_token_keeps_it_plus_marker() {
        let key = "k".repeat(PATTERN_MAX_TEMPLATE_BYTES + 10);
        assert_eq!(
            extract_template(&format!("small {key}=info")).unwrap(),
            "small <_>"
        );
    }

    /// D1 fidelity, the MIRROR of the long-key case (issue #171 round-4
    /// finding): a `key=value` token with a SHORT literal key and a long
    /// digit-free value — `k=` + 511 bytes (513 raw) — renders `k=<_>` (key +
    /// separator literal, value classified at the 64-byte cap), NOT a bare
    /// `<_>`. The retired raw-length gate would have collapsed it; the
    /// per-fragment render lets D1 rule 4 classify only the value (the token's
    /// natural render, `k=<_>`, is 5 bytes — well within the template cap).
    #[test]
    fn a_short_key_long_value_token_renders_key_literal_and_value_classified() {
        let value = "a".repeat(511);
        let body = format!("k={value}");
        assert!(body.len() > PATTERN_MAX_TEMPLATE_BYTES);
        assert_eq!(extract_template(&body).unwrap(), "k=<_>");
    }

    /// D1 rule 5 case (c) — whole tokens that fill the template to EXACTLY the
    /// 512-byte cap produce NO spurious trailing marker: one 64-byte literal
    /// token + seven 63-byte literal tokens + seven spaces = exactly 512 bytes,
    /// every append fits, and the template is the joined tokens verbatim.
    #[test]
    fn rule5_c_tokens_filling_exactly_to_the_cap_emit_no_marker() {
        let mut parts = vec!["a".repeat(64)];
        for _ in 0..7 {
            parts.push("a".repeat(63));
        }
        let body = parts.join(" ");
        assert_eq!(body.len(), PATTERN_MAX_TEMPLATE_BYTES);
        let template = extract_template(&body).unwrap();
        assert_eq!(template, body, "an exact fit keeps every whole token");
        assert!(!template.contains("<_>"), "no spurious truncation marker");
    }

    /// D1 rule 5 case (d) — one token past the exact-fit boundary: the template
    /// stops at the last COMPLETE token and ends in ` <_>`; every retained
    /// segment before the marker is a whole 63-byte token (never a partial).
    #[test]
    fn rule5_d_overflow_by_one_token_keeps_whole_tokens_and_appends_the_marker() {
        // Nine 63-byte literal tokens overflow the cap (9×63 + 8 = 575 > 512).
        let body = std::iter::repeat_n("a".repeat(63), 9)
            .collect::<Vec<_>>()
            .join(" ");
        let template = extract_template(&body).unwrap();
        assert!(template.len() <= PATTERN_MAX_TEMPLATE_BYTES);
        assert!(template.ends_with(" <_>"), "marker at a token boundary");
        let kept = template.strip_suffix(" <_>").unwrap();
        assert!(
            kept.split(' ').all(|t| t == "a".repeat(63)),
            "every retained token is complete, never a partial: {kept:?}"
        );
        assert!(
            kept.split(' ').count() < 9,
            "at least one whole token was dropped to make room"
        );
    }

    /// D1 rule 5 case (e) — multibyte-UTF-8 tokens at the truncation boundary:
    /// no panic, valid UTF-8, correct marker, and a multibyte token is dropped
    /// WHOLE (never byte-sliced mid-codepoint). Two shapes: (e1) a single
    /// multibyte token whose own render exceeds the cap → `<_>`; (e2) a
    /// multibyte token that overflows the byte budget after a fitting token →
    /// the whole multibyte token is dropped and the marker appears.
    #[test]
    fn rule5_e_multibyte_tokens_at_the_boundary_never_panic_and_drop_whole() {
        // (e1) 300 × "é" (600 B) key → natural render > 512 → dropped → "<_>".
        let key = "é".repeat(300);
        assert_eq!(extract_template(&format!("{key}=x")).unwrap(), "<_>");

        // (e2) a 508-byte literal `key=value` token (a 506-byte key keeps its
        // literal bytes — rule 3, unlike a >64-byte UNSPLIT token) then a 4-byte
        // multibyte "éé" token: the second does not fit (508 + 1 + 4 = 513 >
        // 512), so it is dropped WHOLE and " <_>" is appended (508 + 1 + 3 =
        // 512) — the multibyte token is never byte-sliced mid-codepoint.
        let first = format!("{}=v", "k".repeat(506));
        assert_eq!(first.len(), 508);
        let template = extract_template(&format!("{first} éé")).expect("no panic");
        assert_eq!(template, format!("{first} <_>"));
        assert!(template.is_char_boundary(template.len())); // trivially valid UTF-8
    }

    // -- D1 rule 5: token / byte caps ------------------------------------

    #[test]
    fn the_token_cap_truncates_at_a_boundary_with_a_trailing_placeholder() {
        let body = (0..PATTERN_MAX_TOKENS + 5)
            .map(|_| "word")
            .collect::<Vec<_>>()
            .join(" ");
        let template = extract_template(&body).unwrap();
        let toks: Vec<&str> = template.split(' ').collect();
        assert_eq!(
            toks.len(),
            PATTERN_MAX_TOKENS + 1,
            "64 tokens + trailing <_>"
        );
        assert_eq!(*toks.last().unwrap(), PLACEHOLDER);
    }

    #[test]
    fn the_byte_cap_holds_including_the_trailing_placeholder() {
        // Many long digit-free tokens: literal, so bytes accumulate fast.
        let body = (0..200).map(|_| "abcdefghij").collect::<Vec<_>>().join(" ");
        let template = extract_template(&body).unwrap();
        assert!(
            template.len() <= PATTERN_MAX_TEMPLATE_BYTES,
            "template was {} bytes",
            template.len()
        );
        assert!(template.ends_with(PLACEHOLDER));
    }

    // -- D1 rule 1: prefix cut + trailing placeholder --------------------

    #[test]
    fn a_body_over_the_prefix_drops_the_partial_token_and_appends_a_placeholder() {
        // 1024 'a's then " tail": the tail is beyond the prefix and dropped.
        let body = format!("{} tail", "a".repeat(PATTERN_SCAN_PREFIX_BYTES));
        let template = extract_template(&body).unwrap();
        // First token is one 1024-byte run -> over the fragment cap -> <_>.
        assert_eq!(template, "<_> <_>");
    }

    #[test]
    fn the_prefix_cut_is_char_boundary_safe_never_panicking_on_multibyte() {
        // A multibyte char straddling the 1024-byte cut must not panic.
        let mut body = "x".repeat(PATTERN_SCAN_PREFIX_BYTES - 1);
        body.push('é'); // 2 bytes, straddles byte 1024
        body.push_str(" more");
        let template = extract_template(&body).expect("no panic, non-empty");
        assert!(template.ends_with(PLACEHOLDER));
    }

    // -- empty body ------------------------------------------------------

    #[test]
    fn an_empty_or_whitespace_only_body_yields_no_template() {
        assert_eq!(extract_template(""), None);
        assert_eq!(extract_template("   \t\n "), None);
    }

    // -- determinism -----------------------------------------------------

    #[test]
    fn extraction_is_deterministic_same_body_same_template() {
        let body = "GET /api/v1/users/42 status=500 took=1.2ms";
        assert_eq!(extract_template(body), extract_template(body));
    }

    // -- est_template_bound ----------------------------------------------

    #[test]
    fn est_template_bound_never_under_estimates_over_adversarial_bodies() {
        let cases = [
            "1 2 3 4 5 6 7 8 9 0",            // all 1-byte variable tokens
            "a=1 b=2 c=3",                    // key=value
            &"x".repeat(2000),                // over the prefix
            "level=info msg=\"hello world\"", // mixed
            "",                               // empty
            "нет digits here",                // multibyte literal
        ];
        for body in cases {
            let row = log_row(1, 0, body);
            let bound = est_template_bound(&row);
            let actual = extract_template(body).map_or(0, |t| t.len() as u64);
            assert!(
                bound >= actual,
                "under-estimate: bound {bound} < actual {actual} for body {body:?}"
            );
            assert!(bound <= PATTERN_MAX_TEMPLATE_BYTES as u64);
        }
    }

    // -- aggregate_patterns ----------------------------------------------

    #[test]
    fn aggregation_sums_identical_templates_within_a_bucket() {
        let rows = vec![
            log_row(7, 0, "user 1 login"),
            log_row(7, 5_000_000_000, "user 2 login"), // same bucket (0..10s), same template
            log_row(7, 9_000_000_000, "user 3 login"),
        ];
        let agg = aggregate_patterns(&rows);
        assert_eq!(agg.dropped, 0);
        assert_eq!(agg.rows.len(), 1);
        assert_eq!(agg.rows[0].fingerprint, 7);
        assert_eq!(agg.rows[0].bucket_ns, 0);
        assert_eq!(agg.rows[0].pattern, "user <_> login");
        assert_eq!(agg.rows[0].count, 3);
    }

    #[test]
    fn aggregation_separates_buckets_and_fingerprints() {
        let rows = vec![
            log_row(7, 0, "a 1"),
            log_row(7, 10_000_000_000, "a 2"), // next bucket
            log_row(9, 0, "a 3"),              // different fingerprint
        ];
        let agg = aggregate_patterns(&rows);
        assert_eq!(agg.rows.len(), 3);
        // Deterministic sorted order.
        assert_eq!(
            agg.rows
                .iter()
                .map(|r| (r.fingerprint, r.bucket_ns))
                .collect::<Vec<_>>(),
            vec![(7, 0), (7, 10_000_000_000), (9, 0)]
        );
    }

    #[test]
    fn aggregation_skips_empty_body_rows() {
        let rows = vec![log_row(1, 0, ""), log_row(1, 0, "   ")];
        assert_eq!(aggregate_patterns(&rows), PatternAggregation::default());
    }

    #[test]
    fn aggregation_drops_unseen_templates_at_the_distinct_cap_counting_them() {
        // CAP distinct templates then extra unseen ones — the extras drop.
        let mut rows = Vec::new();
        for i in 0..MAX_DISTINCT_PATTERNS_PER_BATCH {
            rows.push(log_row(1, 0, &format!("token_{}_a", alpha(i))));
        }
        // One MORE distinct (unseen) template — must be dropped.
        rows.push(log_row(1, 0, "totally different unseen literal words here"));
        // And a REPEAT of an existing template — must still increment (not
        // dropped, since it is present).
        rows.push(log_row(1, 0, &format!("token_{}_a", alpha(0))));

        let agg = aggregate_patterns(&rows);
        assert_eq!(agg.rows.len(), MAX_DISTINCT_PATTERNS_PER_BATCH);
        assert_eq!(agg.dropped, 1, "only the unseen-at-cap row drops");
        let total: u64 = agg.rows.iter().map(|r| r.count).sum();
        assert_eq!(
            total,
            MAX_DISTINCT_PATTERNS_PER_BATCH as u64 + 1,
            "the repeat of a present template still counts"
        );
    }

    /// Deterministic all-alpha id (no digits, so templates stay literal and
    /// distinct) for the cap test.
    fn alpha(mut n: usize) -> String {
        let mut s = String::new();
        loop {
            s.push((b'a' + (n % 26) as u8) as char);
            n /= 26;
            if n == 0 {
                break;
            }
        }
        s
    }
}
