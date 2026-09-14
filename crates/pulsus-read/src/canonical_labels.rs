//! The ONE reader of PulsusDB's canonical flat-label JSON — the shape the
//! writer stores in `log_streams.labels`, `metric_series.labels` and
//! `log_samples.structured_metadata`: a flat object of string keys to
//! string values, keys sorted, no nesting (docs/architecture.md §2.3).
//!
//! # What this decoder is specified against
//!
//! **Not "JSON" in the abstract — agreement with ClickHouse's own JSON
//! functions on the same stored bytes.** ClickHouse decodes these same
//! columns on paths that run beside ours, and the two answers are compared
//! by the user, not by us:
//!
//! * `log_streams_idx_mv` is built by ClickHouse from the labels column with
//!   `ARRAY JOIN JSONExtractKeysAndValues(labels, 'String')`
//!   (`crates/pulsus-schema/src/catalog.rs:942`). LogQL stage 1 selects
//!   streams through that index; LogQL stage 2 renders the same streams'
//!   labels through this decoder.
//! * The metric label pushdown renders
//!   `JSONExtractString(labels, '<key>') = '<value>'`
//!   (`crates/pulsus-read/src/metrics/series_where.rs:333`), and the
//!   in-process matcher answers the same selector from the label cache,
//!   which is filled through this decoder. Which of the two paths answers a
//!   given query is a runtime decision (`crates/pulsus-read/src/metrics/labels.rs:9-14`),
//!   so the same query can take either — the absent-label rule at
//!   `crates/pulsus-read/src/metrics/labels.rs:629-632` says so in those
//!   words.
//!
//! So a byte this decoder reads differently from `JSONExtractString` is a
//! query that answers differently depending on which path served it. That
//! is what issue #539 was: `\b` and `\f` were missing from the escape
//! table, three copies of the table had the same hole, and a value stored
//! as `a` + U+0008 + `b` read back as the three letters `abb` — one letter
//! away from a different, real value.
//!
//! # The escape table
//!
//! JSON's complete two-character set — `"` `\` `/` `b` `f` `n` `r` `t` —
//! plus `\uXXXX`. [`every_unicode_scalar_value_survives_the_writer_then_the_reader`]
//! walks the whole domain rather than a hand list, because a hand list is
//! what was incomplete.
//!
//! # What this decoder accepts that JSON does not
//!
//! Carried across from the three copies unchanged. None of it is reachable
//! from the writer; it is recorded here so the next reader does not have to
//! re-derive it, and it is the reason this is not `serde_json`:
//!
//! | input | here | `serde_json` | `JSONExtractString` |
//! |---|---|---|---|
//! | `""` (the structured-metadata default, `catalog.rs:441`) | zero pairs | `Err` at column 0 | `''` |
//! | `{"k""v"}` — no colon | `[("k","v")]` | `Err: expected ':'` | `''` |
//! | `{"a":1,"b":"z"}` — a non-string value | `[]`, it stops | both pairs | `'z'` for `b` |
//! | `{"k":"a\qb"}` — an unknown escape | `[("k","aqb")]` | `Err: invalid escape` | `''` |
//! | `{"k":"a\ud800b"}` — a lone surrogate | `[("k","ab")]` | `Err` | `''` |
//!
//! The first row is the one that decides it: our own writer produces the
//! empty string for every log entry with no structured metadata
//! (`crates/pulsus-write/src/protocols/loki_push.rs:788-790`), and
//! `serde_json` rejects it. The rest are unreachable — the only two
//! renderers that reach these columns are
//! `pulsus_model::LabelSet::to_canonical_json` and
//! `pulsus_write::protocols::otlp_logs::push_json_string`, and both bottom
//! out in `serde_json::to_string` over string keys and string values.
//!
//! `serde_json` IS a dependency of this crate (`crates/pulsus-read/Cargo.toml:53`,
//! for the `| json` pipeline stage). Three doc comments used to say the
//! opposite as the reason for hand-writing this; the reason is the table
//! above, and the per-row allocation contract below.
//!
//! # The allocation contract
//!
//! [`parse_canonical_labels_into`] APPENDS into a caller-owned buffer. The
//! structured-metadata merge calls it once per returned log row and reuses
//! one scratch buffer across rows (issues #97 and #249), so a decoder that
//! allocated its own return vector would allocate once per row.
//! [`parse_canonical_labels`] and [`parse_canonical_label_set`] are the two
//! callers that do want a fresh container — a series' labels, once per
//! series.

use pulsus_model::LabelSet;

/// Decodes the canonical flat-label JSON, APPENDING each pair onto `out`.
///
/// Duplicate keys are both kept, in document order; the two `LabelSet`
/// callers collapse them through `from_verbatim`'s winner rule, and the
/// LogQL caller keeps both because a stream's labels and a row's structured
/// metadata are merged by a rule of their own
/// (`crate::logql::labels::merge_labels_with_structured_metadata`).
///
/// Malformed input — which should never occur, this only ever reads back
/// what the writer produced — yields whatever pairs were parsed so far
/// rather than panicking.
pub(crate) fn parse_canonical_labels_into(json: &str, out: &mut Vec<(String, String)>) {
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

/// [`parse_canonical_labels_into`] into a fresh `Vec`.
pub(crate) fn parse_canonical_labels(json: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    parse_canonical_labels_into(json, &mut out);
    out
}

/// [`parse_canonical_labels`] collapsed through
/// [`LabelSet::from_verbatim`] — what both metric readers want, since a
/// series' labels are already normalized keys.
pub(crate) fn parse_canonical_label_set(json: &str) -> LabelSet {
    LabelSet::from_verbatim(parse_canonical_labels(json))
}

fn skip_ws<I: Iterator<Item = char>>(chars: &mut std::iter::Peekable<I>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
}

/// One JSON string literal, from its opening quote to its closing one.
///
/// The escape table is JSON's complete two-character set plus `\uXXXX`.
/// `"`, `\` and `/` decode to themselves, which the catch-all below would
/// also do — they are listed anyway, because the table's completeness is
/// the property that failed in issue #539 and a reader checking it against
/// `RFC 8259 §7` should find all eight arms here.
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
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
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

    /// The write side and the read side of the canonical flat-label format
    /// are two functions, and nothing compared them until issue #539 — the
    /// module that could not decode `\b` rendered it correctly 320 lines
    /// above. This compares them over the WHOLE domain, so an escape added
    /// on one side and not the other cannot pass, and neither can an
    /// escape missing from both in the same way.
    ///
    /// `LabelSet::to_canonical_json` is the writer's own expression, the
    /// one `MetricSeriesRow::from_series_at_bucket` and `From<&StreamRow>`
    /// call (`crates/pulsus-write/src/writer/rows.rs:342` and `:103`), so
    /// this is the stored bytes and not a hand-written literal.
    #[test]
    fn every_unicode_scalar_value_survives_the_writer_then_the_reader() {
        let mut wrong: Vec<u32> = Vec::new();
        for cp in (0u32..=0x10FFFF).filter(|c| !(0xD800..=0xDFFF).contains(c)) {
            let c = char::from_u32(cp).expect("non-surrogate scalar value");
            let value = format!("a{c}b");
            let (set, _) = LabelSet::from_normalized(vec![("k".to_string(), value.clone())]);
            let pairs = parse_canonical_labels(&set.to_canonical_json());
            if pairs.len() != 1 || pairs[0].0 != "k" || pairs[0].1 != value {
                wrong.push(cp);
            }
        }
        assert!(
            wrong.is_empty(),
            "{} scalar value(s) do not round trip: {:04X?}",
            wrong.len(),
            wrong
        );
    }

    /// JSON's eight two-character escapes and `\uXXXX`, named one at a
    /// time so a failure says WHICH escape rather than a code point. The
    /// stored text here is written out by hand — the exhaustive test above
    /// goes through the writer, this one pins the table against the
    /// specification the writer is only one witness of.
    #[test]
    fn the_named_escapes_decode_to_the_characters_json_defines() {
        // (stored text, the decoded character, the escape's name)
        let table: [(&str, char, &str); 9] = [
            (r#"{"k":"a\"b"}"#, '"', r#"\""#),
            (r#"{"k":"a\\b"}"#, '\\', r"\\"),
            (r#"{"k":"a\/b"}"#, '/', r"\/"),
            (r#"{"k":"a\bb"}"#, '\u{8}', r"\b"),
            (r#"{"k":"a\fb"}"#, '\u{c}', r"\f"),
            (r#"{"k":"a\nb"}"#, '\n', r"\n"),
            (r#"{"k":"a\rb"}"#, '\r', r"\r"),
            (r#"{"k":"a\tb"}"#, '\t', r"\t"),
            (r#"{"k":"a\u0041b"}"#, 'A', r"\u0041"),
        ];
        for (stored, want, name) in table {
            // Each row must actually carry the escape it names. The ninth
            // row shipped with a raw `A` where its name promised
            // `\u0041`, so the one arm this row exists to exercise was
            // never reached and the row asserted that the letter A decodes
            // to itself. These two checks are what catch that class of
            // mistake: a name is an escape, and the stored text contains it.
            assert!(
                name.starts_with('\\'),
                "row {name:?} does not name an escape; an escape starts with a backslash"
            );
            assert!(
                stored.contains(name),
                "the row for {name} must STORE that escape, and {stored} does not"
            );
            let pairs = parse_canonical_labels(stored);
            assert_eq!(
                pairs,
                vec![("k".to_string(), format!("a{want}b"))],
                "the escape {name} in {stored} must decode to U+{:04X}",
                want as u32
            );
        }
    }

    /// **Three of the eight arms cannot be seen by any behaviour test.**
    /// `"`, `\\` and `/` decode to themselves, which the catch-all
    /// `other => out.push(other)` also does, so deleting all three leaves
    /// every other test in this module green — measured, issue #539. They
    /// are kept because the table's COMPLETENESS is the property that
    /// failed, and this test reads the source text, which is the only
    /// instrument that can see them.
    ///
    /// The needles are assembled at run time, so the literals in this test
    /// do not match themselves and report a deleted arm as present.
    #[test]
    fn the_escape_table_lists_all_eight_of_jsons_two_character_escapes() {
        // Scoped to the PARSER's own body, not the whole file: over the
        // file, an arm deleted from the table and a look-alike written
        // anywhere else — in a test, in a doc comment's code block — would
        // keep the count at one and the test green. A code review asked
        // for this scoping.
        let src = parser_source();
        // The SOURCE spelling of each escape's match pattern: a backslash
        // is written `\\` in a Rust character literal.
        for pattern in ["\"", "\\\\", "/", "b", "f", "n", "r", "t"] {
            let needle = format!("'{pattern}' => out.push(");
            assert_eq!(
                src.matches(&needle).count(),
                1,
                "the escape table must carry exactly one arm matching {needle:?} — JSON \
                 defines eight two-character escapes and issue #539 was two of them missing"
            );
        }
    }

    /// The source text of `parse_json_string` alone — from its declaration
    /// at column 0 to its closing brace at column 0 — so a check over the
    /// escape table reads the table and nothing else.
    ///
    /// The declaration it looks for is assembled at run time, so this
    /// function's own source does not contain it.
    fn parser_source() -> &'static str {
        let src = include_str!("canonical_labels.rs");
        let decl = format!("{}fn parse_{}", '\n', "json_string");
        let start = src.find(&decl).expect("this module declares the parser") + 1;
        let rest = &src[start..];
        let end = rest
            .find("\n}\n")
            .expect("the parser's closing brace at column 0")
            + 2;
        let body = &rest[..end];
        assert!(
            body.len() > 200 && body.ends_with("\n}"),
            "the parser slice is {} byte(s) and does not end at a closing brace in column 0",
            body.len()
        );
        body
    }

    /// The two code points issue #539 fixed, each between its immediate
    /// neighbours — which come back through a DIFFERENT mechanism, so a
    /// build that special-cased U+0008 alone still fails here:
    ///
    /// ```text
    ///   U+0007   \u0007   the \uXXXX arm
    ///   U+0008   \b       the arm added by #539
    ///   U+0009   \t       an explicit arm
    ///   U+000B   \u000b   the \uXXXX arm
    ///   U+000C   \f       the arm added by #539
    ///   U+000D   \r       an explicit arm
    /// ```
    ///
    /// Both readings of each value are in the corpus AT ONCE — `a\bb`
    /// stores the three characters `a`, U+0008, `b`, and `abb` stores the
    /// three letters — so a wrong build is caught by two stored values
    /// colliding on one decoded value, not only by one value going
    /// missing.
    #[test]
    fn the_boundary_neighbours_of_the_two_added_escapes_round_trip() {
        for cp in [0x07u32, 0x08, 0x09, 0x0B, 0x0C, 0x0D] {
            let c = char::from_u32(cp).expect("a C0 control is a scalar value");
            let value = format!("a{c}b");
            let (set, _) = LabelSet::from_normalized(vec![
                ("control".to_string(), value.clone()),
                ("letters".to_string(), "abb".to_string()),
            ]);
            let stored = set.to_canonical_json();
            let pairs = parse_canonical_labels(&stored);
            assert_eq!(
                pairs,
                vec![
                    ("control".to_string(), value.clone()),
                    ("letters".to_string(), "abb".to_string()),
                ],
                "U+{cp:04X} stored as {stored} must decode back to itself, and must not \
                 collide with the three letters abb"
            );
            assert_ne!(
                pairs[0].1, pairs[1].1,
                "U+{cp:04X} and the letter it is escaped with must stay two different values"
            );
        }
    }

    /// Issue #539's first item: ONE decoder — checked by SEARCHING the
    /// crate, not by reading a list of files.
    ///
    /// The first version of this test named the three files that used to
    /// carry a private copy and read those three with `include_str!`. A
    /// code review pointed out what that cannot see: a fourth copy in any
    /// of the crate's other source files leaves it green. It now walks
    /// every `.rs` file under this crate's `src/` and reports WHERE each
    /// definition is, so a new copy anywhere fails with its own path.
    ///
    /// The needle is assembled at run time from two pieces, so that the
    /// literal it looks for does not occur in this test's own source and
    /// count itself.
    #[test]
    fn the_crate_defines_the_json_string_parser_exactly_once() {
        let needle = format!("fn parse_{}", "json_string");
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        collect_rust_sources(&root, &mut files);
        files.sort();
        // The walk is the instrument, so it is checked too: this crate had
        // 86 source files when this was written, and a walk that found a
        // handful would pass the census below while seeing almost nothing.
        assert!(
            files.len() > 50,
            "the walk found {} source file(s) under {} — it is not walking the crate",
            files.len(),
            root.display()
        );
        let mut definers: Vec<String> = Vec::new();
        for path in &files {
            let src = std::fs::read_to_string(path)
                .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
            for _ in 0..src.matches(&needle).count() {
                definers.push(
                    path.strip_prefix(&root)
                        .unwrap_or(path)
                        .display()
                        .to_string(),
                );
            }
        }
        assert_eq!(
            definers,
            vec!["canonical_labels.rs".to_string()],
            "the crate must define the JSON string parser exactly once and here; issue #539 \
             collapsed three private copies into this module because the escape table in all \
             three had the same hole"
        );
    }

    /// Every `.rs` file under `dir`, recursively — the finder the census
    /// above uses.
    fn collect_rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                collect_rust_sources(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }

    #[test]
    fn parse_canonical_labels_reads_simple_pairs() {
        let pairs = parse_canonical_labels(r#"{"env":"prod","team":"checkout"}"#);
        assert_eq!(
            pairs,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "checkout".to_string())
            ]
        );
    }

    #[test]
    fn parse_canonical_labels_handles_escaped_quotes_and_backslashes() {
        let pairs = parse_canonical_labels(r#"{"msg":"a\"b\\c"}"#);
        assert_eq!(pairs, vec![("msg".to_string(), "a\"b\\c".to_string())]);
    }

    #[test]
    fn parse_canonical_labels_of_empty_object_is_empty() {
        assert!(parse_canonical_labels("{}").is_empty());
    }

    /// The structured-metadata column defaults to the empty string
    /// (`crates/pulsus-schema/src/catalog.rs:441`) and the log-push
    /// receiver writes it for every entry that carries no metadata, so
    /// this is the common case, not an edge case. `serde_json` rejects it.
    #[test]
    fn the_empty_string_is_zero_pairs_and_not_an_error() {
        assert!(parse_canonical_labels("").is_empty());
        let mut buf = vec![("kept".to_string(), "yes".to_string())];
        parse_canonical_labels_into("", &mut buf);
        assert_eq!(buf, vec![("kept".to_string(), "yes".to_string())]);
    }

    /// The appending contract (issues #97, #249): the caller's buffer is
    /// filled, never replaced.
    #[test]
    fn parse_canonical_labels_into_appends_to_the_callers_buffer() {
        let mut buf = vec![("base".to_string(), "1".to_string())];
        parse_canonical_labels_into(r#"{"sm":"2"}"#, &mut buf);
        assert_eq!(
            buf,
            vec![
                ("base".to_string(), "1".to_string()),
                ("sm".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn parse_canonical_label_set_collapses_into_a_label_set() {
        let set = parse_canonical_label_set(r#"{"job":"api","env":"prod"}"#);
        assert_eq!(set.get("job"), Some("api"));
        assert_eq!(set.get("env"), Some("prod"));
    }

    #[test]
    fn parse_canonical_label_set_of_empty_object_is_empty() {
        assert!(parse_canonical_label_set("{}").is_empty());
    }
}
