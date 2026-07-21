//! Issue #124 (M7-A6): the `{{...}}` native-histogram sample-literal
//! grammar, ported from the pinned `promql/parser/generated_parser.y`
//! (`histogram_series_value`/`histogram_desc_map`/`histogram_desc_item`/
//! `bucket_set` productions) and `promql/parser/parse.go`
//! (`buildHistogramFromMap`/`buildHistogramBucketsAndSpans`), both at
//! `40af9c2` (`git show 40af9c2:promql/parser/{generated_parser.y,
//! parse.go}`). Builds [`pulsus_model::FloatHistogram`] DIRECTLY — no
//! integer-`NativeHistogram` detour, matching the pin (upstream's own
//! `histogram.FloatHistogram` grammar target).
//!
//! Supported descriptor keys (`histogram_desc_item`): `schema`, `sum`,
//! `count`, `z_bucket`, `z_bucket_w`, `custom_values`, `buckets`, `offset`,
//! `n_buckets`, `n_offset`, `counter_reset_hint`. Since issue #125 the
//! hint is KEPT: it is written into the built
//! [`pulsus_model::FloatHistogram`], and [`parse_histogram_literal`]
//! additionally returns `hint_set: bool` — whether the literal spelled a
//! `counter_reset_hint:` key at all (the pin's
//! `lastHistogramCounterResetHintSet`, `parser/parse.go:161-164,627`).
//! The comparator (`runner.rs::histogram_almost_equal`) asserts the hint
//! ONLY when an EXPECTED literal set one, mirroring the pin's
//! `compareNativeHistogram(…, counterResetHintSet)`; load lines ignore
//! `hint_set` but keep the hint value itself (a gauge-hinted load series
//! becomes a gauge chunk in the store's read-back emulation).
//!
//! Combinators (`series_item`'s `histogram_series_value [TIMES uint |
//! ADD histogram_series_value TIMES uint | SUB histogram_series_value
//! TIMES uint]`): a bare `{{...}}xN` repeats N+1 times (the "additional
//! value for time 0" convention every sequence-value form in this corpus
//! shares); `{{A}}+{{B}}xN` / `{{A}}-{{B}}xN` fold N times via
//! [`FloatHistogram::add`]/[`FloatHistogram::sub`], mirroring
//! `histogramsIncreaseSeries`/`histogramsDecreaseSeries` (`parse.go:517-536`)
//! including the pin's own schema-compatibility guard (the accumulator's
//! schema must never exceed the increment's — `parse.go:544-546`).

use pulsus_model::{CombineOp, CounterResetHint, FloatHistogram, Span};

use super::series::{SeqValue, scan_signed_number};

/// One `histogram_desc_item` accumulation — upstream's dynamically-typed
/// `map[string]any`, typed here since every key has one fixed Rust type.
#[derive(Debug, Default)]
struct HistogramDesc {
    schema: Option<i32>,
    sum: Option<f64>,
    count: Option<f64>,
    z_bucket: Option<f64>,
    z_bucket_w: Option<f64>,
    custom_values: Option<Vec<f64>>,
    buckets: Option<Vec<f64>>,
    offset: Option<i32>,
    n_buckets: Option<Vec<f64>>,
    n_offset: Option<i32>,
    /// Issue #125: `Some` iff the literal spelled `counter_reset_hint:` —
    /// carries both the parsed hint and the pin's `counterResetHintSet`.
    counter_reset_hint: Option<CounterResetHint>,
}

/// Parses one `{{...}}` histogram literal from the front of `s` (which
/// must start with `{{`), returning the built [`FloatHistogram`] — with
/// `hint_set: bool`, whether the literal spelled a `counter_reset_hint:`
/// key (issue #125; the pin's `counterResetHintSet`) — and the remainder
/// of `s` starting right after the matching `}}`. Mirrors
/// `histogram_series_value` + `buildHistogramFromMap`.
#[allow(clippy::type_complexity)]
pub fn parse_histogram_literal(s: &str) -> Result<((FloatHistogram, bool), &str), String> {
    let after_open = s
        .strip_prefix("{{")
        .ok_or_else(|| format!("expected '{{{{' at {s:?}"))?;
    // Content never legitimately contains the literal substring "}}" (no
    // descriptor value can produce it — bucket lists and numbers are the
    // only free-form content), so the first occurrence is always the
    // terminator.
    let end = after_open
        .find("}}")
        .ok_or_else(|| format!("unterminated histogram literal (missing '}}}}') in {s:?}"))?;
    let content = &after_open[..end];
    let rest = &after_open[end + 2..];
    let desc = parse_desc_map(content)?;
    Ok((build_histogram_from_map(desc), rest))
}

fn build_histogram_from_map(desc: HistogramDesc) -> (FloatHistogram, bool) {
    let (positive_buckets, positive_spans) =
        build_buckets_and_spans(desc.buckets, desc.offset.unwrap_or(0));
    let (negative_buckets, negative_spans) =
        build_buckets_and_spans(desc.n_buckets, desc.n_offset.unwrap_or(0));
    let hint_set = desc.counter_reset_hint.is_some();
    let h = FloatHistogram {
        counter_reset_hint: desc.counter_reset_hint.unwrap_or_default(),
        schema: desc.schema.unwrap_or(0),
        zero_threshold: desc.z_bucket_w.unwrap_or(0.0),
        zero_count: desc.z_bucket.unwrap_or(0.0),
        count: desc.count.unwrap_or(0.0),
        sum: desc.sum.unwrap_or(0.0),
        positive_spans,
        negative_spans,
        positive_buckets,
        negative_buckets,
        custom_values: desc.custom_values.unwrap_or_default(),
    };
    (h, hint_set)
}

/// `buildHistogramBucketsAndSpans` (`parse.go:669-694`): a non-empty
/// bucket list becomes exactly one span `{offset, length}`.
fn build_buckets_and_spans(buckets: Option<Vec<f64>>, offset: i32) -> (Vec<f64>, Vec<Span>) {
    match buckets {
        Some(buckets) if !buckets.is_empty() => {
            let spans = vec![Span {
                offset,
                length: buckets.len() as u32,
            }];
            (buckets, spans)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

/// Parses the `key:value (SPACE key:value)*` content between `{{`/`}}` —
/// `histogram_desc_map`/`histogram_desc_item`. Empty (or whitespace-only)
/// content is the empty histogram (`{{}}`/`{{ }}`).
fn parse_desc_map(content: &str) -> Result<HistogramDesc, String> {
    let mut desc = HistogramDesc::default();
    let mut rest = content.trim();
    while !rest.is_empty() {
        let (key, after_key) = rest
            .split_once(':')
            .ok_or_else(|| format!("expected 'key:value' in histogram descriptor, at {rest:?}"))?;
        let key = key.trim();
        let after_key = after_key.trim_start();
        rest = match key {
            "schema" => {
                let (v, r) = scan_signed_int(after_key)?;
                reject_duplicate("schema", desc.schema.replace(v))?;
                r
            }
            "sum" => {
                let (v, r) = scan_number(after_key)?;
                reject_duplicate("sum", desc.sum.replace(v))?;
                r
            }
            "count" => {
                let (v, r) = scan_number(after_key)?;
                reject_duplicate("count", desc.count.replace(v))?;
                r
            }
            "z_bucket" => {
                let (v, r) = scan_number(after_key)?;
                reject_duplicate("z_bucket", desc.z_bucket.replace(v))?;
                r
            }
            "z_bucket_w" => {
                let (v, r) = scan_number(after_key)?;
                reject_duplicate("z_bucket_w", desc.z_bucket_w.replace(v))?;
                r
            }
            "custom_values" => {
                let (v, r) = scan_bucket_set(after_key)?;
                reject_duplicate("custom_values", desc.custom_values.replace(v))?;
                r
            }
            "buckets" => {
                let (v, r) = scan_bucket_set(after_key)?;
                reject_duplicate("buckets", desc.buckets.replace(v))?;
                r
            }
            "offset" => {
                let (v, r) = scan_signed_int(after_key)?;
                reject_duplicate("offset", desc.offset.replace(v))?;
                r
            }
            "n_buckets" => {
                let (v, r) = scan_bucket_set(after_key)?;
                reject_duplicate("n_buckets", desc.n_buckets.replace(v))?;
                r
            }
            "n_offset" => {
                let (v, r) = scan_signed_int(after_key)?;
                reject_duplicate("n_offset", desc.n_offset.replace(v))?;
                r
            }
            "counter_reset_hint" => {
                // Issue #125: parsed AND kept — the pin's closed keyword
                // set (`parse.go:630-641`); an explicit `unknown` still
                // counts as "set" for the comparator (the pin sets
                // `lastHistogramCounterResetHintSet = true` on the key,
                // not on the value).
                let (kw, r) = scan_ident(after_key)?;
                let hint = match kw {
                    "unknown" => CounterResetHint::Unknown,
                    "reset" => CounterResetHint::CounterReset,
                    "not_reset" => CounterResetHint::NotCounterReset,
                    "gauge" => CounterResetHint::Gauge,
                    _ => {
                        return Err(format!(
                            "invalid counter_reset_hint {kw:?} (want unknown/reset/not_reset/gauge)"
                        ));
                    }
                };
                reject_duplicate("counter_reset_hint", desc.counter_reset_hint.replace(hint))?;
                r
            }
            other => {
                return Err(format!(
                    "unknown histogram descriptor key {other:?} in {content:?}"
                ));
            }
        };
        rest = rest.trim_start();
    }
    Ok(desc)
}

fn reject_duplicate<T>(key: &str, previous: Option<T>) -> Result<(), String> {
    if previous.is_some() {
        Err(format!("duplicate key {key:?} in histogram descriptor"))
    } else {
        Ok(())
    }
}

/// `signed_or_unsigned_number` — reuses the series-value float scanner
/// (Inf/NaN/decimal/exponent, signed).
fn scan_number(s: &str) -> Result<(f64, &str), String> {
    scan_signed_number(s).ok_or_else(|| format!("expected a number at {s:?}"))
}

/// `int` — a bare signed integer (schema/offset/n_offset never carry a
/// fraction or exponent in the pin's grammar).
fn scan_signed_int(s: &str) -> Result<(i32, &str), String> {
    let (sign, body) = match s.as_bytes().first() {
        Some(b'-') => (-1i64, &s[1..]),
        Some(b'+') => (1i64, &s[1..]),
        _ => (1i64, s),
    };
    let end = body
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit())
        .map(|(i, _)| i)
        .unwrap_or(body.len());
    if end == 0 {
        return Err(format!("expected an integer at {s:?}"));
    }
    let n: i64 = body[..end]
        .parse()
        .map_err(|e| format!("invalid integer {s:?}: {e}"))?;
    let v = sign * n;
    let v = i32::try_from(v).map_err(|_| format!("integer {v} out of range for {s:?}"))?;
    Ok((v, &body[end..]))
}

/// A bare lowercase identifier (`counter_reset_hint`'s keyword value).
fn scan_ident(s: &str) -> Result<(&str, &str), String> {
    let end = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '_')
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    if end == 0 {
        return Err(format!("expected an identifier at {s:?}"));
    }
    Ok((&s[..end], &s[end..]))
}

/// `bucket_set`: `[ bucket_set_list ]` where `bucket_set_list` is
/// `signed_or_unsigned_number (SPACE signed_or_unsigned_number)*` — at
/// least ONE number. The pin's grammar (`generated_parser.y`
/// `bucket_set_list`, ~:1075-1084) has NO empty alternative, so an empty
/// `[]` is a PARSE ERROR (upstream never accepts `buckets:[]`); shared by
/// every bucket-list key (`custom_values`/`buckets`/`n_buckets`).
fn scan_bucket_set(s: &str) -> Result<(Vec<f64>, &str), String> {
    let s = s.trim_start();
    let mut rest = s
        .strip_prefix('[')
        .ok_or_else(|| format!("expected '[' at {s:?}"))?;
    let mut vals = Vec::new();
    loop {
        rest = rest.trim_start();
        if let Some(r) = rest.strip_prefix(']') {
            if vals.is_empty() {
                return Err(format!(
                    "empty bucket list {s:?} — a bucket list requires at least one number \
                     (upstream grammar has no empty alternative)"
                ));
            }
            return Ok((vals, r));
        }
        let (v, r) = scan_number(rest)?;
        vals.push(v);
        rest = r;
    }
}

/// One `series_item` built from `{{...}}` at the front of `s`: a bare
/// literal, `{{...}}xN`, `{{A}}+{{B}}xN`, or `{{A}}-{{B}}xN`. Returns the
/// expanded [`SeqValue::Histogram`] sequence and the remainder of `s`.
pub fn parse_histogram_series_item(s: &str) -> Result<(Vec<SeqValue>, &str), String> {
    let ((first, hint_set), rest) = parse_histogram_literal(s)?;
    if let Some(after_x) = rest.strip_prefix('x') {
        let (n, rest2) = scan_uint(after_x)?;
        return Ok((
            vec![SeqValue::Histogram(first, hint_set); n as usize + 1],
            rest2,
        ));
    }
    if let Some(after_op) = rest.strip_prefix('+')
        && after_op.starts_with("{{")
    {
        return combine_series(first, after_op, CombineOp::Add);
    }
    if let Some(after_op) = rest.strip_prefix('-')
        && after_op.starts_with("{{")
    {
        return combine_series(first, after_op, CombineOp::Sub);
    }
    Ok((vec![SeqValue::Histogram(first, hint_set)], rest))
}

/// `histogram_series_value ADD/SUB histogram_series_value TIMES uint` —
/// `histogramsIncreaseSeries`/`histogramsDecreaseSeries` (`parse.go:517-559`).
/// The `hint_set` flag applied to EVERY produced value is the INC
/// literal's ("Capture the hint set flag immediately after inc histogram
/// is built", `parse.go:520-523,530-532`), and each accumulation step's
/// hint merges through the model `add`/`sub` (gauge + gauge ⇒ gauge —
/// the pin's `histogramsIncreaseSeries` folds via `Add`, issue #125).
fn combine_series(
    base: FloatHistogram,
    after_op: &str,
    op: CombineOp,
) -> Result<(Vec<SeqValue>, &str), String> {
    let ((inc, hint_set), rest) = parse_histogram_literal(after_op)?;
    let after_x = rest
        .strip_prefix('x')
        .ok_or_else(|| format!("expected 'x<count>' after histogram combinator, at {rest:?}"))?;
    let (times, rest2) = scan_uint(after_x)?;

    let mut ret = Vec::with_capacity(times as usize + 1);
    ret.push(SeqValue::Histogram(base.clone(), hint_set));
    let mut cur = base;
    for _ in 0..times {
        if cur.schema > inc.schema {
            return Err(format!(
                "error combining histograms: cannot merge from schema {} to {}",
                inc.schema, cur.schema
            ));
        }
        let outcome = match op {
            CombineOp::Add => cur.add(&inc),
            CombineOp::Sub => cur.sub(&inc),
        }
        .map_err(|e| e.to_string())?;
        cur = outcome.result;
        ret.push(SeqValue::Histogram(cur.clone(), hint_set));
    }
    Ok((ret, rest2))
}

fn scan_uint(s: &str) -> Result<(u64, &str), String> {
    let end = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit())
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    if end == 0 {
        return Err(format!("expected a repeat count at {s:?}"));
    }
    let n: u64 = s[..end]
        .parse()
        .map_err(|e| format!("invalid repeat count {s:?}: {e}"))?;
    Ok((n, &s[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_literal_parses_to_a_zeroed_histogram() {
        let ((h, hint_set), rest) = parse_histogram_literal("{{}}").unwrap();
        assert!(!hint_set, "no counter_reset_hint key => hint_set false");
        assert_eq!(rest, "");
        assert_eq!(h.schema, 0);
        assert_eq!(h.sum, 0.0);
        assert_eq!(h.count, 0.0);
        assert!(h.positive_buckets.is_empty());
        assert!(h.negative_buckets.is_empty());
    }

    #[test]
    fn full_descriptor_populates_every_field() {
        let ((h, _), rest) =
            parse_histogram_literal("{{schema:0 sum:5 count:4 buckets:[1 2 1]}} trailing").unwrap();
        assert_eq!(rest, " trailing");
        assert_eq!(h.schema, 0);
        assert_eq!(h.sum, 5.0);
        assert_eq!(h.count, 4.0);
        assert_eq!(h.positive_buckets, vec![1.0, 2.0, 1.0]);
        assert_eq!(
            h.positive_spans,
            vec![Span {
                offset: 0,
                length: 3
            }]
        );
    }

    #[test]
    fn negative_custom_and_offset_fields_parse() {
        let ((h, _), _) = parse_histogram_literal(
            "{{schema:-53 custom_values:[-2 3] n_buckets:[1 2] n_offset:-1}}",
        )
        .unwrap();
        assert_eq!(h.schema, -53);
        assert_eq!(h.custom_values, vec![-2.0, 3.0]);
        assert_eq!(h.negative_buckets, vec![1.0, 2.0]);
        assert_eq!(
            h.negative_spans,
            vec![Span {
                offset: -1,
                length: 2
            }]
        );
    }

    /// Issue #125: the hint is parsed AND kept — value on the histogram,
    /// `hint_set` reported (even for an explicit `unknown`, the pin's
    /// key-not-value rule).
    #[test]
    fn counter_reset_hint_is_parsed_and_kept_with_hint_set() {
        for (kw, want) in [
            ("unknown", pulsus_model::CounterResetHint::Unknown),
            ("reset", pulsus_model::CounterResetHint::CounterReset),
            ("not_reset", pulsus_model::CounterResetHint::NotCounterReset),
            ("gauge", pulsus_model::CounterResetHint::Gauge),
        ] {
            let literal = format!("{{{{sum:1 count:1 counter_reset_hint:{kw}}}}}");
            let ((h, hint_set), rest) = parse_histogram_literal(&literal).unwrap();
            assert_eq!(rest, "");
            assert_eq!(h.sum, 1.0);
            assert_eq!(h.counter_reset_hint, want, "{kw}");
            assert!(hint_set, "{kw}: an explicit key always sets hint_set");
        }
        let ((_, hint_set), _) = parse_histogram_literal("{{sum:1 count:1}}").unwrap();
        assert!(!hint_set);
    }

    #[test]
    fn unknown_key_is_a_loud_error() {
        let err = parse_histogram_literal("{{bogus:1}}").unwrap_err();
        assert!(err.contains("bogus"), "{err}");
    }

    /// Codex A6 review [low]: an empty bucket list `[]` is a PARSE ERROR
    /// in the pin (`generated_parser.y` `bucket_set_list` has no empty
    /// alternative), never a silently-empty bucket vector. Non-vacuous:
    /// this input parsed to an empty `positive_buckets` before the fix.
    /// Covers every bucket-list key (they share `scan_bucket_set`).
    #[test]
    fn empty_bucket_list_is_a_parse_error() {
        for literal in [
            "{{schema:0 buckets:[]}}",
            "{{schema:0 n_buckets:[]}}",
            "{{schema:-53 custom_values:[]}}",
        ] {
            let err = parse_histogram_literal(literal).unwrap_err();
            assert!(err.contains("empty bucket list"), "{literal}: {err}");
        }
        // A valid, non-empty bucket list still parses — no over-rejection.
        let ((h, _), _) = parse_histogram_literal("{{schema:0 buckets:[1]}}").unwrap();
        assert_eq!(h.positive_buckets, vec![1.0]);
        let ((h, _), _) = parse_histogram_literal("{{schema:0 n_buckets:[0 5]}}").unwrap();
        assert_eq!(h.negative_buckets, vec![0.0, 5.0]);
    }

    #[test]
    fn unterminated_literal_is_a_loud_error() {
        let err = parse_histogram_literal("{{schema:0").unwrap_err();
        assert!(err.contains("unterminated"), "{err}");
    }

    #[test]
    fn bare_repeat_expands_to_n_plus_one_copies() {
        let (vals, rest) = parse_histogram_series_item("{{sum:1 count:1}}x3").unwrap();
        assert_eq!(rest, "");
        assert_eq!(vals.len(), 4);
        for v in &vals {
            let SeqValue::Histogram(h, _) = v else {
                panic!("expected a histogram value")
            };
            assert_eq!(h.sum, 1.0);
        }
    }

    #[test]
    fn increase_series_accumulates_via_add() {
        let (vals, rest) = parse_histogram_series_item(
            "{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:2 count:1 buckets:[1] offset:1}}x2",
        )
        .unwrap();
        assert_eq!(rest, "");
        assert_eq!(vals.len(), 3);
        let SeqValue::Histogram(first, _) = &vals[0] else {
            panic!()
        };
        assert_eq!(first.sum, 4.0);
        let SeqValue::Histogram(second, _) = &vals[1] else {
            panic!()
        };
        assert_eq!(second.sum, 6.0);
        assert_eq!(second.count, 5.0);
        let SeqValue::Histogram(third, _) = &vals[2] else {
            panic!()
        };
        assert_eq!(third.sum, 8.0);
        assert_eq!(third.count, 6.0);
    }

    #[test]
    fn decrease_series_accumulates_via_sub() {
        let (vals, _) = parse_histogram_series_item(
            "{{schema:0 sum:10 count:4 buckets:[4]}}-{{sum:1 count:1 buckets:[1]}}x2",
        )
        .unwrap();
        let SeqValue::Histogram(last, _) = &vals[2] else {
            panic!()
        };
        assert_eq!(last.sum, 8.0);
        assert_eq!(last.count, 2.0);
    }

    #[test]
    fn combining_from_a_higher_to_a_lower_schema_is_a_loud_error() {
        let err =
            parse_histogram_series_item("{{schema:2 sum:1 count:1}}+{{schema:0 sum:1 count:1}}x1")
                .unwrap_err();
        assert!(err.contains("cannot merge from schema"), "{err}");
    }
}
