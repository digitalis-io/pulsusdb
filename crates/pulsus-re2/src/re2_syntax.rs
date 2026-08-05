//! Issue #317: rewriting a user regex so the Rust `regex` crate reads it
//! the way **RE2** does.
//!
//! Upstream Prometheus (the metrics API's reference of record, issue #283)
//! compiles every label-matcher regex with Go's `regexp` — an RE2 port —
//! and this engine's storage path hands the same pattern to ClickHouse's
//! RE2 (issue #280, which made RE2 the authority on *acceptance*). The
//! in-process paths that ROUTE THROUGH THIS MODULE — the warm label
//! cache, `concrete_name_matches`, `info()`'s ignore-set matchers,
//! `label_replace` on both signals — compile with the Rust
//! `regex` crate instead. The two grammars overlap without either
//! containing the other, and several constructs they both accept mean
//! different things.
//!
//! **`docs/api.md` §9.2 is the authoritative table of which constructs
//! those are, and this module exists to close exactly that list.** It is
//! deliberately not restated here. The copy that used to live in this
//! comment drifted twice before issue #336 caught it — it claimed `[]a]`
//! reads differently in the two engines (it does not; the rewrite escapes
//! it anyway, a harmless no-op) and listed `(?P<n>…)` as an acceptance
//! divergence (both engines accept named groups). Change a rewrite rule
//! and update §9.2, never a second table.
//!
//! The RULES this module applies, which is a statement about the code
//! rather than about either engine: the Perl classes `\d`/`\w`/`\s` and
//! their negations, the boundaries `\b`/`\B`, the class-set operators
//! (`&&`, `~~`, `--`), a nested `[`, a leading `]`, and a brace run that
//! is not a well-formed repetition. Everything else passes through
//! byte-for-byte.
//!
//! These are value divergences, not status ones — an unrewritten pattern
//! makes the query SUCCEED and return the wrong rows, with nothing to
//! indicate it.
//!
//! NOT every in-process regex site routes through here. The callers that
//! do are the four named above; `pulsus-read`'s LogQL pipeline compiles
//! the user's pattern with the Rust crate and does NOT call
//! [`re2_pattern_to_rust`], which is the open defect behind docs/api.md
//! §9.1's "as written" rows (issue #336). Adding a caller means adding it
//! to that list, not assuming it.
//!
//! The rewrite is
//! applied **only to the Rust side**: the pattern that reaches ClickHouse
//! is still the user's, because RE2 already reads it correctly and
//! rewriting the SQL predicate could only add risk. The one exception is
//! issue #331's [`clickhouse_match_strategy`], below — not a reading
//! difference at all, but a defect in ClickHouse's own `match()`
//! pre-filter that makes certain flag-group heads silently select no
//! rows; the SQL side must render those patterns differently or return
//! empty results. The differential
//! (`pulsus-read/tests/re2_screen_differential.rs`) is the evidence for
//! both: for every corpus pattern both engines accept, `regex` over the
//! rewrite, RE2 over the original, and ClickHouse `match()` over the
//! rendered literal agree on every probe subject.
//!
//! **Not** in scope here — acceptance divergences, where one engine
//! rejects what the other compiles, in either direction (docs/api.md
//! §9.3 and §9.4 tabulate both). Those are `metrics::re2_authority`'s
//! job: it screens them off the in-process path so RE2 returns the
//! verdict. A rewrite whose output the Rust crate rejects therefore costs
//! a storage round-trip and never a wrong answer.

use std::borrow::Cow;

/// RE2's ASCII definitions of the Perl classes (`re2/parse.cc`
/// `kPerlDigit`/`kPerlSpace`/`kPerlWord`; Go
/// `regexp/syntax/perl_groups.go`). Note `\s` has **no** vertical tab —
/// it is `[\t\n\f\r ]`, not POSIX `space`.
fn ascii_perl_class(escape: char) -> Option<&'static str> {
    Some(match escape {
        'd' => "[0-9]",
        'D' => "[^0-9]",
        'w' => "[0-9A-Za-z_]",
        'W' => "[^0-9A-Za-z_]",
        's' => r"[\t\n\f\r ]",
        'S' => r"[^\t\n\f\r ]",
        _ => return None,
    })
}

/// Rewrites `pattern` so the Rust `regex` crate reads it the way RE2 does.
///
/// Borrowed unchanged unless the pattern carries a backslash, a character
/// class or a brace — the only three bytes any rewrite rule keys off — so
/// an ordinary matcher (`api|web`, `prod-.+`) costs one scan and no
/// allocation. Never fails: a pattern neither engine can compile is passed
/// through so the compiler, not this pass, produces the error.
pub fn re2_pattern_to_rust(pattern: &str) -> Cow<'_, str> {
    if !pattern
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b'\\' | b'[' | b'{' | b'}'))
    {
        return Cow::Borrowed(pattern);
    }
    // Indexed `char`s rather than bytes: a class item may be a multi-byte
    // literal and the `-`/`]` lookahead has to land on character
    // boundaries. One allocation per rewritten pattern, on a path that is
    // about to compile a regex (µs) and, at every call site, memoizes the
    // result.
    let cs: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut i = 0;
    while i < cs.len() {
        i = match cs[i] {
            '\\' => push_escape(&cs, i, &mut out),
            '[' => push_class(&cs, i, &mut out),
            '{' => push_brace(&cs, i, &mut out),
            // A `}` that did not close a repetition: a literal in RE2, and
            // `push_brace` has already escaped its opener.
            '}' => {
                out.push_str(r"\}");
                i + 1
            }
            c => {
                out.push(c);
                i + 1
            }
        };
    }
    Cow::Owned(out)
}

/// One escape sequence **outside** a character class, starting at the
/// backslash `cs[i]`. Returns the index just past it.
fn push_escape(cs: &[char], i: usize, out: &mut String) -> usize {
    // `\p{…}`/`\pN` first: its braces must never reach `push_brace`.
    if let Some(end) = unicode_class_end(cs, i) {
        out.extend(&cs[i..=end]);
        return end + 1;
    }
    let Some(&next) = cs.get(i + 1) else {
        // A trailing lone backslash — malformed in both engines; passed
        // through so the compiler says so.
        out.push('\\');
        return i + 1;
    };
    if let Some(ascii) = ascii_perl_class(next) {
        out.push_str(ascii);
        return i + 2;
    }
    // The crate's ASCII-boundary syntax, which is exactly RE2's `\b`.
    if next == 'b' || next == 'B' {
        out.push_str(if next == 'b' {
            r"(?-u:\b)"
        } else {
            r"(?-u:\B)"
        });
        return i + 2;
    }
    let end = escape_span_end(cs, i);
    out.extend(&cs[i..end]);
    end
}

/// A `{` outside a character class. RE2 reads a brace that does not open a
/// well-formed `{n}`/`{n,}`/`{n,m}` as a **literal** (`a{bbb}c`, `a{,5}`);
/// the Rust crate rejects it as a malformed repetition, so those patterns
/// could only ever be answered by storage. Escaping the literal case makes
/// both engines read the same thing.
fn push_brace(cs: &[char], i: usize, out: &mut String) -> usize {
    match repetition_end(cs, i) {
        // A real repetition is copied whole, so its `}` never reaches the
        // stray-brace rule.
        Some(end) => {
            out.extend(&cs[i..=end]);
            end + 1
        }
        None => {
            out.push_str(r"\{");
            i + 1
        }
    }
}

/// The index of the `}` closing a well-formed repetition opened at `cs[i]`
/// (`{n}`, `{n,}`, `{n,m}` — digits required before any comma, exactly Go's
/// `parseRepeat`). `None` when the brace is a literal in RE2.
fn repetition_end(cs: &[char], open: usize) -> Option<usize> {
    let mut i = open + 1;
    let mut digits = 0usize;
    let mut seen_comma = false;
    loop {
        match cs.get(i) {
            Some(c) if c.is_ascii_digit() => {
                digits += 1;
                i += 1;
            }
            Some(',') if !seen_comma && digits > 0 => {
                seen_comma = true;
                digits = 0;
                i += 1;
            }
            // `{n,}` needs no upper bound; every other form needs digits.
            Some('}') if digits > 0 || seen_comma => return Some(i),
            _ => return None,
        }
    }
}

/// Rewrites the character class opened at `cs[open]`, returning the index
/// just past its `]` (or past the end, for an unterminated class both
/// engines reject).
///
/// Walks items exactly as RE2 does (`re2/parse.cc` `ParseCharClass`, Go
/// `regexp/syntax` `parseClass`) and re-emits each one so the Rust crate
/// cannot read it as something else:
///
/// * a leading `]` is a literal, not the terminator (`first`);
/// * `-` is a range operator **only** when a character follows it and that
///   character is not `]` — every other `-` is a literal and is escaped, so
///   `--` can never reach the Rust crate as its difference operator;
/// * `&`, `~`, `[` and `^` are literals and are escaped, so `&&`/`~~` are
///   not set operators and `[` does not open a nested class.
fn push_class(cs: &[char], open: usize, out: &mut String) -> usize {
    out.push('[');
    let mut i = open + 1;
    if cs.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    let mut first = true;
    loop {
        let Some(&c) = cs.get(i) else {
            // Unterminated: "missing closing ]" in RE2, and the Rust crate
            // rejects it too. Nothing more to emit.
            return i;
        };
        if c == ']' && !first {
            out.push(']');
            return i + 1;
        }
        first = false;

        // `[:alpha:]` and `\p{…}` mean the same in both engines; copied
        // whole so their punctuation is never mistaken for an item.
        if let Some(end) = posix_class_end(cs, i).or_else(|| unicode_class_end(cs, i)) {
            out.extend(&cs[i..=end]);
            i = end + 1;
            continue;
        }
        // `\b`/`\B` are deliberately NOT rewritten here: inside a class
        // they are not boundaries, and — unlike Perl — RE2 does not read
        // them as BACKSPACE either. It rejects them (`invalid escape
        // sequence: \b`, measured on ClickHouse 24.8), exactly as the Rust
        // crate does, so copying them through keeps the two engines'
        // rejections aligned. Issue #68 mapped them to `\x08` on Perl's
        // reading; the issue #317 differential caught that as a pattern
        // this engine would answer and RE2 would refuse.
        if c == '\\'
            && let Some(&next) = cs.get(i + 1)
            && let Some(ascii) = ascii_perl_class(next)
        {
            out.push_str(ascii);
            i += 2;
            continue;
        }

        let lo_end = escape_span_end(cs, i);
        let is_range =
            cs.get(lo_end) == Some(&'-') && matches!(cs.get(lo_end + 1), Some(&n) if n != ']');
        push_class_literal(&cs[i..lo_end], out);
        if is_range {
            out.push('-');
            let hi_end = escape_span_end(cs, lo_end + 1);
            push_class_literal(&cs[lo_end + 1..hi_end], out);
            i = hi_end;
        } else {
            i = lo_end;
        }
    }
}

/// One class item that RE2 reads as a literal character. An already-escaped
/// item is copied verbatim; a bare one is escaped when the Rust crate would
/// otherwise read it as class syntax.
fn push_class_literal(item: &[char], out: &mut String) {
    if item.len() != 1 {
        out.extend(item);
        return;
    }
    let c = item[0];
    if matches!(c, '\\' | ']' | '[' | '^' | '-' | '&' | '~') {
        out.push('\\');
    }
    out.push(c);
}

/// The index just past the single character (or escape sequence) starting
/// at `cs[i]` — `\x41` and `\x{263A}` are one character, not two tokens, so
/// the `-` after them is classified against the right position.
fn escape_span_end(cs: &[char], i: usize) -> usize {
    if cs.get(i) != Some(&'\\') {
        return i + 1;
    }
    match cs.get(i + 1) {
        None => i + 1,
        Some('x') => match cs.get(i + 2) {
            Some('{') => match cs[i + 3..].iter().position(|&c| c == '}') {
                Some(offset) => i + 3 + offset + 1,
                None => cs.len(),
            },
            _ => (i + 4).min(cs.len()),
        },
        Some(_) => i + 2,
    }
}

/// The index of the `]` closing a POSIX class `[:name:]` opened at `cs[i]`.
fn posix_class_end(cs: &[char], i: usize) -> Option<usize> {
    if cs.get(i) != Some(&'[') || cs.get(i + 1) != Some(&':') {
        return None;
    }
    let mut j = i + 2;
    while j + 1 < cs.len() {
        if cs[j] == ':' && cs[j + 1] == ']' {
            return Some(j + 1);
        }
        j += 1;
    }
    None
}

/// The index of the last character of a Unicode class `\p{…}`/`\P{…}`/
/// `\pN` starting at `cs[i]`.
fn unicode_class_end(cs: &[char], i: usize) -> Option<usize> {
    if cs.get(i) != Some(&'\\') || !matches!(cs.get(i + 1), Some('p' | 'P')) {
        return None;
    }
    match cs.get(i + 2) {
        Some('{') => cs[i + 3..]
            .iter()
            .position(|&c| c == '}')
            .map(|offset| i + 3 + offset),
        Some(_) => Some(i + 2),
        None => None,
    }
}

// ---------------------------------------------------------------------
// Issue #331: ClickHouse `match()` leaks a flag-group head into its
// required-substring optimisation.
// ---------------------------------------------------------------------

/// How a pattern must be rendered for ClickHouse's `match()` (issue
/// #331).
///
/// ClickHouse's `OptimizedRegularExpression::analyze` extracts a
/// required substring from the pattern and refuses rows that do not
/// contain it before RE2 ever runs. A `(?…` **flag-group head whose
/// flags carry no `i`** corrupts that extraction — the head's own
/// characters leak into the required literal — so the predicate
/// silently selects no rows where RE2 matches. Measured on 24.8.14.39:
/// `match('xaby', '(?s:ab)')` is `0` while `match('xs:aby',
/// '(?s:ab)')` is `1` — the server is requiring the literal `s:ab`.
/// Every flag-and-form combination over `{s,m,U}`, scoped (`(?s:`) or
/// flag-only (`(?s)`), positive or negated (`(?-m)`), at any position,
/// misbehaves; heads carrying an `i` anywhere (`(?i:`, `(?s-i:`,
/// `(?-si)`), plain `(?:`/`(?)`, and named groups `(?P<`/`(?<` do not.
///
/// The classification here deliberately **over-approximates**: the
/// server's exact leak condition also depends on surrounding
/// alternation and literal length (a lone `(?s)a` happens to survive),
/// but both remedies below are semantic no-ops, so applying one to a
/// pattern that would have survived costs only the optimisation — while
/// under-approximating would leave a silently empty result set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickhouseMatchStrategy {
    /// No affected head: the pattern must be rendered **byte-for-byte
    /// as it always was**, keeping the required-substring optimisation.
    Verbatim,
    /// Affected heads, **no `i` in any flag head of the pattern, and
    /// the pattern is literal-leading** (its first concrete element,
    /// after anchors and group/flag-head openers, is a literal
    /// character): render this text instead. Each affected head has
    /// had `i` appended to its **negated** flags (`(?s:` → `(?s-i:`,
    /// `(?-m)` → `(?-mi)`), which the analyzer handles correctly.
    /// Clearing `i` where nothing can have set it is a semantic no-op,
    /// and both preconditions are checked by the classifier, not
    /// assumed.
    ///
    /// **The remedy is routed by shape, and the shapes were measured**
    /// by the committed artifact — `cargo xtask bench match-flag-head`
    /// (`xtask/src/bench/match_flag_head.rs`: deterministic data,
    /// stated protocol, calibration buckets, literal-leading AND
    /// `.*`-wrapped spellings of every core). What round 1's and the
    /// review's benchmarks actually disagreed about was the pattern
    /// SHAPE, not the environment (the reviewer's run of this artifact
    /// reproduced these buckets): for literal-leading patterns the
    /// rewrite keeps the analyzer's prefilter on both machines
    /// measured (anchored ~35 ms vs the arm's ~82–95 ms full-RE2
    /// bucket, ~2.4×), while a leading `.*` INSIDE the rewritten flag
    /// group demotes it to the full-RE2 bucket where it ran BEHIND the
    /// arm at five of six shape-points (worst observed ~14%, not a
    /// bound) — so non-literal-leading patterns take the arm instead.
    /// Where the literal is present in ~every row the prefilter prunes
    /// nothing and the arm led by up to ~14% even literal-leading
    /// (worst observed); selectivity is unknowable at render time and
    /// the selective case is what filters exist for. The artifact's
    /// coverage of both spellings is what makes this routing checkable
    /// — extend it before trusting any new shape.
    RewriteHeads(String),
    /// Affected heads where the rewrite's preconditions fail: an
    /// `i`-carrying head coexists (the `-i` edit could flip a
    /// genuinely inherited case-insensitivity and cannot be proven a
    /// no-op locally), or the pattern is not literal-leading (measured
    /// by the artifact: the rewrite loses its prefilter there and runs
    /// behind this arm). The renderer must append a never-matching
    /// alternative (`|$.` — end-of-text followed by one more
    /// character) at the top level, outside any group enclosing the
    /// user's pattern so user flags cannot scope into it. A top-level
    /// alternation makes the analyzer abandon the required substring,
    /// and RE2 then answers exactly the user's pattern. Cost: the
    /// full-RE2 bucket of the committed artifact on every machine
    /// measured so far — flat and predictable, which is exactly its
    /// virtue: it has no precondition to be right about. (At the
    /// measured dotstar points today's broken rendering happened NOT
    /// to leak; the transform is kept regardless, because
    /// under-approximating the leak risks the silent empty result this
    /// issue exists to fix.)
    NeverMatchArm,
}

/// Classifies `pattern` for ClickHouse `match()` rendering. See
/// [`ClickhouseMatchStrategy`]. One byte scan when the pattern cannot
/// contain a head at all (no `(?`), so ordinary matchers pay nothing.
pub fn clickhouse_match_strategy(pattern: &str) -> ClickhouseMatchStrategy {
    if !pattern.contains("(?") {
        return ClickhouseMatchStrategy::Verbatim;
    }
    let cs: Vec<char> = pattern.chars().collect();
    let (leaking, any_i_head) = scan_flag_heads(&cs);
    if leaking.is_empty() {
        return ClickhouseMatchStrategy::Verbatim;
    }
    if any_i_head || !starts_with_required_literal(&cs) {
        return ClickhouseMatchStrategy::NeverMatchArm;
    }
    ClickhouseMatchStrategy::RewriteHeads(build_head_rewrite(&cs, &leaking))
}

/// The `-i` head rewrite with the SHAPE routing ignored: `Some` whenever
/// the pattern carries affected heads and the no-op precondition holds
/// (no `i` in any flag head), `None` otherwise. Semantically identical
/// to the pattern either way.
///
/// This is a MEASUREMENT seam, not a routing decision: production
/// rendering must go through [`clickhouse_match_strategy`], which
/// applies the shape gate. It exists so `cargo xtask bench
/// match-flag-head` can measure BOTH remedies at every shape-point —
/// fix round 4: the round-3 routing change silently broke the bench,
/// which had assumed the strategy always answered `RewriteHeads` for
/// its cores.
pub fn clickhouse_match_head_rewrite(pattern: &str) -> Option<String> {
    if !pattern.contains("(?") {
        return None;
    }
    let cs: Vec<char> = pattern.chars().collect();
    let (leaking, any_i_head) = scan_flag_heads(&cs);
    if leaking.is_empty() || any_i_head {
        return None;
    }
    Some(build_head_rewrite(&cs, &leaking))
}

/// One scan: every valid flag head, split into the affected (no-`i`)
/// list and an any-`i`-head flag. Shared by the strategy and the
/// measurement seam so the two can never disagree about what a head is.
fn scan_flag_heads(cs: &[char]) -> (Vec<FlagHead>, bool) {
    let mut leaking: Vec<FlagHead> = Vec::new();
    let mut any_i_head = false;
    let mut i = 0;
    while i < cs.len() {
        match cs[i] {
            '\\' if cs.get(i + 1) == Some(&'Q') => i = quote_end(cs, i),
            '\\' => i = escape_span_end(cs, i),
            '[' => i = class_span_end(cs, i),
            '(' if cs.get(i + 1) == Some(&'?') => match parse_flag_head(cs, i) {
                Some(head) => {
                    if head.has_i {
                        any_i_head = true;
                    } else if head.flag_count > 0 {
                        leaking.push(head);
                    }
                    i = head.terminator + 1;
                }
                // `(?P<`, `(?<`, or a head RE2 will reject loudly:
                // never edited, so an invalid pattern stays invalid.
                None => i += 2,
            },
            _ => i += 1,
        }
    }
    (leaking, any_i_head)
}

/// Appends `i` to each affected head's negated flags. No `i` in any
/// flag head means case-insensitivity is off at every point of the
/// pattern (RE2 has no other way to set it), so clearing it inside each
/// affected head changes nothing — the callers gate on that.
fn build_head_rewrite(cs: &[char], leaking: &[FlagHead]) -> String {
    let mut out = String::with_capacity(cs.len() + 2 * leaking.len());
    let mut next = leaking.iter().peekable();
    for (idx, &c) in cs.iter().enumerate() {
        if next.peek().is_some_and(|h| h.terminator == idx) {
            out.push_str(if next.next().expect("peeked").has_minus {
                "i"
            } else {
                "-i"
            });
        }
        out.push(c);
    }
    out
}

/// A valid RE2 flag-group head `(?flags[-flags]` ending in `:` or `)`.
#[derive(Clone, Copy)]
struct FlagHead {
    /// Index of the closing `:` or `)`.
    terminator: usize,
    /// Total flags on either side of the `-`.
    flag_count: usize,
    /// `i` anywhere in the head — either side of the `-`.
    has_i: bool,
    /// The head already carries a negation part.
    has_minus: bool,
}

/// Parses the flag-group head opened by `(?` at `cs[open]`. `None` for
/// named groups and for anything RE2 itself rejects (`(?x…`, `(?-)`,
/// `(?-:` — those must reach the server untouched so the rejection
/// stays loud). `(?:` and `(?)` parse with `flag_count == 0` and are
/// never rewritten.
fn parse_flag_head(cs: &[char], open: usize) -> Option<FlagHead> {
    let mut j = open + 2;
    if matches!(cs.get(j), Some('P' | '<')) {
        return None;
    }
    let mut flag_count = 0usize;
    let mut has_i = false;
    let mut has_minus = false;
    while let Some(&c) = cs.get(j) {
        match c {
            'i' | 'm' | 's' | 'U' => {
                flag_count += 1;
                has_i |= c == 'i';
                j += 1;
            }
            '-' if !has_minus => {
                has_minus = true;
                j += 1;
                // RE2 requires at least one flag after the `-`.
                if !matches!(cs.get(j), Some('i' | 'm' | 's' | 'U')) {
                    return None;
                }
            }
            ':' | ')' => {
                // A bare `(?-…` with no leading flags is fine (`(?-s)`),
                // but `-` with no flags at all was rejected above.
                return Some(FlagHead {
                    terminator: j,
                    flag_count,
                    has_i,
                    has_minus,
                });
            }
            _ => return None,
        }
    }
    None
}

/// `true` when the pattern provably REQUIRES its first concrete
/// literal character in every match — the shape gate for
/// [`ClickhouseMatchStrategy::RewriteHeads`] (fix round 3; tightened in
/// fix round 4 after review attacked it with optional-izing shapes).
/// Three conditions, all conservative:
///
/// 1. walking past `^`, `\A`, group/flag-head/named openers and a
///    leading `\Q`, the first concrete element is a literal character
///    (an ordinary char, or an escape denoting one — `\x41`, `\.`,
///    `\n`, …); `.`, classes, Perl-class escapes and anything
///    unrecognised fail here;
/// 2. that literal is not made optional by what follows it: a `?`, `*`
///    or `{` after the literal (or after a one-character `\Q…\E`
///    quotation) fails; `+` keeps the requirement and passes;
/// 3. nothing elsewhere can route a match around it: any `|` outside a
///    class/quotation fails (round-1 probes measured alternation
///    suppressing the analyzer's extraction anyway), and so does any
///    group closer followed by `?`/`*`/`{` (`(ab)?c` — the group that
///    contained the literal is optional).
///
/// Deliberately over-broad in the failing direction: misclassifying
/// toward the arm costs only the flat full-RE2 bucket both remedies
/// would land in anyway, while claiming the rewrite for a shape with no
/// required leading literal is exactly the mistake the routing exists
/// to prevent. The property is attacked, not enumerated, in
/// `a_claimed_leading_literal_is_genuinely_required`: a seeded
/// generator over optional-izing constructs asserts that whenever this
/// gate passes, the compiled pattern matches NO subject lacking the
/// claimed character.
fn starts_with_required_literal(cs: &[char]) -> bool {
    leading_literal_char(cs).is_some() && !has_optionalizing_structure(cs)
}

/// The pattern's first concrete literal character — DECODED, so the
/// attack test can use it as an oracle (`\x41` claims `A`, `\n` claims
/// the newline) — when there is one to claim (conditions 1-2 of
/// [`starts_with_required_literal`]).
fn leading_literal_char(cs: &[char]) -> Option<char> {
    let mut i = 0;
    while i < cs.len() {
        match cs[i] {
            '^' => i += 1,
            '\\' if matches!(cs.get(i + 1), Some('A')) => i += 2,
            '\\' if matches!(cs.get(i + 1), Some('Q')) => {
                // A quotation: its first character is the literal. A
                // one-character quotation can be quantified away by
                // what follows the `\E`, so only a longer quotation is
                // immune to condition 2.
                let c = *cs.get(i + 2)?;
                if c == '\\' && cs.get(i + 3) == Some(&'E') {
                    return None; // empty quotation
                }
                let end = quote_end(cs, i);
                let quoted_len = end.saturating_sub(i + 2).saturating_sub(2);
                if quoted_len >= 2 || !optionalized_by_quantifier(cs, end) {
                    return Some(c);
                }
                return None;
            }
            '\\' => {
                let decoded = decode_literal_escape(cs, i)?;
                if optionalized_by_quantifier(cs, escape_span_end(cs, i)) {
                    return None;
                }
                return Some(decoded);
            }
            '(' if cs.get(i + 1) == Some(&'?') => {
                if let Some(head) = parse_flag_head(cs, i) {
                    i = head.terminator + 1;
                } else if matches!(cs.get(i + 2), Some('P' | '<')) {
                    match cs[i..].iter().position(|&c| c == '>') {
                        Some(off) => i += off + 1,
                        None => return None,
                    }
                } else {
                    return None;
                }
            }
            '(' => i += 1,
            c => {
                if matches!(c, '.' | '[' | '|' | '$' | '*' | '+' | '?' | '{' | ')' | '}') {
                    return None;
                }
                if optionalized_by_quantifier(cs, i + 1) {
                    return None;
                }
                return Some(c);
            }
        }
    }
    None
}

/// Condition 2's follower rule at the position just past the literal:
/// ANY quantifier character fails the claim. `?`, `*` and `{0…}` make
/// the literal optional outright; `+` alone would keep it required, but
/// quantifiers stack (`a+*`, `b+??{0}` — both matched the empty subject
/// under attack, re-optionalizing through the stack), and no finite
/// lookahead closes that class — so the claim is refused rather than
/// chased. A quantified leading literal routes to the arm; speed-only.
fn optionalized_by_quantifier(cs: &[char], after: usize) -> bool {
    matches!(cs.get(after), Some('?' | '*' | '{' | '+'))
}

/// The single character a leading escape denotes, or `None` for
/// class-like escapes (`\d`), boundaries, and anything not confidently
/// decodable — those fail the claim rather than guessing.
fn decode_literal_escape(cs: &[char], i: usize) -> Option<char> {
    match cs.get(i + 1)? {
        'n' => Some('\n'),
        't' => Some('\t'),
        'r' => Some('\r'),
        'f' => Some('\u{000C}'),
        'v' => Some('\u{000B}'),
        'a' => Some('\u{0007}'),
        'x' => {
            // `\x41` or `\x{...}`: decode, or refuse the claim.
            let hex: String = if cs.get(i + 2) == Some(&'{') {
                let close = cs[i + 3..].iter().position(|&c| c == '}')?;
                cs[i + 3..i + 3 + close].iter().collect()
            } else {
                cs.get(i + 2..i + 4)?.iter().collect()
            };
            char::from_u32(u32::from_str_radix(&hex, 16).ok()?)
        }
        c @ ('.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' | '-'
        | '\\' | '/') => Some(*c),
        _ => None,
    }
}

/// Condition 3 of [`starts_with_required_literal`]: an alternation bar,
/// or a group closer quantified with `?`/`*`/`{`, anywhere outside a
/// class or `\Q…\E` quotation — either can make a match that avoids
/// the leading literal (`(?s:(a|.*)b)`, `(?s:(ab)?c)`).
fn has_optionalizing_structure(cs: &[char]) -> bool {
    let mut i = 0;
    while i < cs.len() {
        match cs[i] {
            '\\' if cs.get(i + 1) == Some(&'Q') => i = quote_end(cs, i),
            '\\' => i = escape_span_end(cs, i),
            '[' => i = class_span_end(cs, i),
            '|' => return true,
            ')' if matches!(cs.get(i + 1), Some('?' | '*' | '{')) => return true,
            _ => i += 1,
        }
    }
    false
}

/// The index just past the `\E` closing the `\Q` at `cs[i]` — or the end
/// of the pattern, which RE2 treats as an implicitly closed quotation.
fn quote_end(cs: &[char], i: usize) -> usize {
    let mut j = i + 2;
    while j + 1 < cs.len() {
        if cs[j] == '\\' && cs[j + 1] == 'E' {
            return j + 2;
        }
        j += 1;
    }
    cs.len()
}

/// The index just past the `]` closing the class opened at `cs[open]`,
/// stepping items the way RE2 reads them (leading-`]` literal, escapes,
/// POSIX classes whose spelling contains `]`, `\Q…\E`) so a `(?` inside
/// a class is never mistaken for a head. Span-only sibling of
/// [`push_class`]; issue #328's `pulsus-re2` crate is where the two
/// walkers merge.
fn class_span_end(cs: &[char], open: usize) -> usize {
    let mut i = open + 1;
    if cs.get(i) == Some(&'^') {
        i += 1;
    }
    let mut first = true;
    loop {
        let Some(&c) = cs.get(i) else {
            return i;
        };
        if c == ']' && !first {
            return i + 1;
        }
        first = false;
        if let Some(end) = posix_class_end(cs, i).or_else(|| unicode_class_end(cs, i)) {
            i = end + 1;
            continue;
        }
        if c == '\\' && cs.get(i + 1) == Some(&'Q') {
            i = quote_end(cs, i);
            continue;
        }
        i = escape_span_end(cs, i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rewrite must be a no-op for ordinary matchers — both for
    /// correctness and because a `Cow::Borrowed` is the whole cost story.
    #[test]
    fn ordinary_patterns_are_borrowed_unchanged() {
        for pattern in [
            "", ".*", "foo", "api|web", "(a|b)c", "(?i)foo", "prod-.+", "10.0.0.1", "a*?",
        ] {
            assert!(
                matches!(re2_pattern_to_rust(pattern), Cow::Borrowed(p) if p == pattern),
                "{pattern:?} must be borrowed unchanged"
            );
        }
    }

    #[test]
    fn perl_classes_become_their_ascii_definitions() {
        assert_eq!(re2_pattern_to_rust(r"\d+"), "[0-9]+");
        assert_eq!(re2_pattern_to_rust(r"\D"), "[^0-9]");
        assert_eq!(re2_pattern_to_rust(r"\w"), "[0-9A-Za-z_]");
        assert_eq!(re2_pattern_to_rust(r"\W"), "[^0-9A-Za-z_]");
        assert_eq!(re2_pattern_to_rust(r"\s"), r"[\t\n\f\r ]");
        assert_eq!(re2_pattern_to_rust(r"\S"), r"[^\t\n\f\r ]");
        assert_eq!(re2_pattern_to_rust(r"\d[\w]\\S"), r"[0-9][[0-9A-Za-z_]]\\S");
    }

    #[test]
    fn word_boundaries_become_the_ascii_form() {
        assert_eq!(re2_pattern_to_rust(r"\bx\B"), r"(?-u:\b)x(?-u:\B)");
    }

    /// Inside a class, `\b`/`\B` are not boundaries and — unlike Perl —
    /// not BACKSPACE either: RE2 rejects them, so they must stay
    /// uncompilable here rather than being rewritten into something the
    /// Rust crate would accept.
    #[test]
    fn class_interior_boundaries_stay_rejected_like_re2() {
        assert_eq!(re2_pattern_to_rust(r"[a\b]x\b"), r"[a\b]x(?-u:\b)");
        for pattern in [r"[\b]", r"[a\b]", r"[\B]"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_err(),
                "{pattern:?} -> {rewritten:?} must stay uncompilable"
            );
        }
    }

    /// An escaped backslash is a literal, so the letter after it is not an
    /// escape — the pass must never rewrite it.
    #[test]
    fn an_escaped_backslash_hides_the_class_that_follows() {
        assert_eq!(re2_pattern_to_rust(r"\\d"), r"\\d");
        assert_eq!(re2_pattern_to_rust(r"\\\d"), r"\\[0-9]");
        assert_eq!(re2_pattern_to_rust(r"\\b"), r"\\b");
    }

    /// Multi-character escapes are single tokens: splitting `\x{263A}`
    /// would hand `{263A}` to the brace rule and corrupt the pattern.
    #[test]
    fn multi_character_escapes_are_copied_as_units() {
        for pattern in [
            r"\x41",
            r"\x{263A}",
            r"\p{L}",
            r"\p{Greek}",
            r"\pN",
            r"\P{Nd}",
        ] {
            assert_eq!(re2_pattern_to_rust(pattern), pattern, "{pattern:?}");
        }
        assert_eq!(re2_pattern_to_rust(r"[\x41-\x5A]"), r"[\x41-\x5A]");
    }

    /// The class set operators the Rust crate has and RE2 does not.
    #[test]
    fn class_set_operators_are_escaped_into_literals() {
        assert_eq!(re2_pattern_to_rust("[a&&b]"), r"[a\&\&b]");
        assert_eq!(re2_pattern_to_rust("[a~~b]"), r"[a\~\~b]");
        assert_eq!(re2_pattern_to_rust("[a[b]]"), r"[a\[b]]");
        assert_eq!(re2_pattern_to_rust("[[a]]"), r"[\[a]]");
    }

    /// RE2 reads `-` as a range operator whenever a character other than
    /// `]` follows it, and as a literal otherwise. Reproducing that rule
    /// exactly is what stops `--` reaching the Rust crate as its difference
    /// operator — including the case where the resulting range is
    /// backwards, which both engines must then reject.
    #[test]
    fn dashes_keep_re2s_range_reading() {
        assert_eq!(re2_pattern_to_rust("[a-z]"), "[a-z]");
        assert_eq!(re2_pattern_to_rust("[a-]"), r"[a\-]");
        assert_eq!(re2_pattern_to_rust("[-a]"), r"[\-a]");
        assert_eq!(re2_pattern_to_rust("[--a]"), r"[\--a]");
        // `a-` then hi `-`: a backwards range, rejected by both engines.
        assert_eq!(re2_pattern_to_rust("[a--b]"), r"[a-\-b]");
        // Built via `format!` so `clippy::invalid_regex` does not reject
        // the deliberately-uncompilable literal at lint time (the #68
        // precedent in `eval::labels`' own tests).
        assert!(regex::Regex::new(&format!("[a-{}-b]", '\\')).is_err());
        // …but after a completed range the `-` is a fresh literal item and
        // the NEXT `-` opens a range, exactly as RE2 reads it.
        assert_eq!(re2_pattern_to_rust("[0-9--4]"), r"[0-9\--4]");
    }

    /// Pins the rewrite's OUTPUT for a leading `]` — the class walker's
    /// indexing depends on the escape being emitted. Deliberately says
    /// nothing about what either engine reads there; docs/api.md §9.2
    /// covers that, and the claim that used to sit here was wrong.
    #[test]
    fn a_leading_close_bracket_is_a_literal() {
        assert_eq!(re2_pattern_to_rust("[]a]"), r"[\]a]");
        assert_eq!(re2_pattern_to_rust("[^]a]"), r"[^\]a]");
    }

    #[test]
    fn posix_classes_pass_through() {
        assert_eq!(re2_pattern_to_rust("[[:alpha:]]"), "[[:alpha:]]");
        assert_eq!(re2_pattern_to_rust("[^[:alpha:]0-9]"), "[^[:alpha:]0-9]");
    }

    /// A brace that does not open a repetition is a literal in RE2 and a
    /// syntax error in the Rust crate.
    #[test]
    fn literal_braces_are_escaped_and_real_repetitions_are_not() {
        assert_eq!(re2_pattern_to_rust("a{bbb}c"), r"a\{bbb\}c");
        assert_eq!(re2_pattern_to_rust("a{,5}"), r"a\{,5\}");
        assert_eq!(re2_pattern_to_rust("a{2}"), "a{2}");
        assert_eq!(re2_pattern_to_rust("a{2,}"), "a{2,}");
        assert_eq!(re2_pattern_to_rust("a{2,3}b{x}"), r"a{2,3}b\{x\}");
        assert_eq!(re2_pattern_to_rust("a{1001}"), "a{1001}");
        // Inside a class a brace is a literal in both engines already.
        assert_eq!(re2_pattern_to_rust("[{}]"), "[{}]");
        for pattern in ["a{bbb}c", "a{,5}", "a}b"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_ok(),
                "{pattern:?} -> {rewritten:?} must compile"
            );
        }
    }

    /// An unterminated class is rejected by both engines; the pass must not
    /// turn it into something that compiles.
    #[test]
    fn unterminated_constructs_are_left_uncompilable() {
        for pattern in ["[abc", "[", "[^", r"foo\", "[a-"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_err(),
                "{pattern:?} -> {rewritten:?} must stay uncompilable"
            );
        }
    }

    // --- issue #331: `clickhouse_match_strategy` -----------------------

    use super::ClickhouseMatchStrategy as S;

    fn strategy(p: &str) -> S {
        clickhouse_match_strategy(p)
    }

    fn rewritten(p: &str) -> String {
        match strategy(p) {
            S::RewriteHeads(out) => out,
            other => panic!("{p:?}: expected RewriteHeads, got {other:?}"),
        }
    }

    /// Patterns with no flag-group head — including every `(?` spelling
    /// that is not one — must render exactly as today.
    #[test]
    fn headless_and_safe_head_patterns_are_verbatim() {
        for p in [
            "",
            ".*",
            "api|web",
            "prod-.+",
            "^a$",
            "(a)(b)",
            "(?:ab)",
            "(?:a|b)c",
            "(?)ab",
            "(?P<n>ab)",
            "(?<n>ab)",
            "(?P<n.x>a)",
            // Heads RE2 itself rejects are left alone so the rejection
            // stays loud and quotes the user's own text.
            "(?x)a b",
            "(?q:ab)",
            "(?-)ab",
            "(?-:ab)",
            "(?s-i-m:ab)",
            "(?R)a",
            "(?#c)a",
            "(?",
            "a(?",
            // A head-shaped sequence inside a class or a `\Q…\E`
            // quotation is literal text, not a head.
            "[(?s:]ab",
            r"[^(?m)]",
            r"\Q(?s:ab\E",
            r"\Q(?s:ab",
            r"a\(?s:b",
            // `i` anywhere in the head keeps the analyzer honest
            // (measured), so these render unchanged.
            "(?i)ab",
            "(?i:ab)",
            "(?im:ab)",
            "(?si:ab)",
            "(?i-s:ab)",
            "(?s-i:ab)",
            "(?-si)ab",
            "(?imsU)a",
        ] {
            assert_eq!(strategy(p), S::Verbatim, "{p:?}");
        }
    }

    /// Every measured leaking family gets the `-i` appended to its
    /// negated flags — creating the negation when absent.
    #[test]
    fn no_i_flag_heads_are_rewritten_with_a_cleared_i() {
        for (p, want) in [
            ("(?s:ab)", "(?s-i:ab)"),
            ("(?m:ab)", "(?m-i:ab)"),
            ("(?U:ab)", "(?U-i:ab)"),
            ("(?sm:ab)", "(?sm-i:ab)"),
            ("(?smU:ab)", "(?smU-i:ab)"),
            ("(?ss:ab)", "(?ss-i:ab)"),
            ("(?s)ab", "(?s-i)ab"),
            ("(?m)ab", "(?m-i)ab"),
            ("(?U)ab", "(?U-i)ab"),
            ("(?-s)ab", "(?-si)ab"),
            ("(?-m:ab)", "(?-mi:ab)"),
            ("(?s-m:ab)", "(?s-mi:ab)"),
            ("(?U-s)ab", "(?U-si)ab"),
            ("x(?s:ab)y", "x(?s-i:ab)y"),
            ("(?s:a(?m:b))", "(?s-i:a(?m-i:b))"),
            // A leading non-empty quotation is literal text, so the
            // real head after it is still rewritten.
            (r"\Q(?s:\E(?s:ab)", r"\Q(?s:\E(?s-i:ab)"),
        ] {
            assert_eq!(rewritten(p), want, "{p:?}");
        }
    }

    /// An `i`-carrying head anywhere in the pattern disqualifies the
    /// local `-i` rewrite (it could flip an inherited case-insensitivity),
    /// so the renderer must fall back to the never-matching arm.
    #[test]
    fn mixed_i_and_leaking_heads_need_the_never_match_arm() {
        for p in [
            "(?i)(?s:ab)",
            "(?s:ab)(?i)cd",
            "(?i:(?s:ab))",
            "(?s:(?i:ab))",
            "(?m:a)(?-i)b",
        ] {
            assert_eq!(strategy(p), S::NeverMatchArm, "{p:?}");
        }
    }

    /// Fix round 3: a pattern that is not literal-leading takes the arm
    /// — the artifact measured the rewrite losing its prefilter (and
    /// running behind the arm) exactly there. Class-leading and
    /// escape-class-leading shapes route conservatively to the arm too.
    #[test]
    fn non_literal_leading_patterns_take_the_never_match_arm() {
        for p in [
            "(?s:.*ab.*)",
            ".*(?s:ab)",
            "(?s:.+ab)",
            r"(?s:\d+ab)",
            r"\w(?s:ab)",
            "[ab](?s:cd)",
            "[(?s:]a(?m:b)",
            "(?s:[a]b)",
            r"\Q\E(?s:ab)",
            // Fix round 4 (review attack): shapes whose leading literal
            // is not REQUIRED — an alternation that can route around
            // it, or a quantifier that makes it optional.
            "(?s:a)|(?m:b)",
            "(?s:(a|.*)b)",
            "(?s:(?:a|.*)b)",
            "(?s:(a|[0-9])b)",
            "(?s:a?bc)",
            "(?s:a*bc)",
            "(?s:a{0}bc)",
            "(?s:a{2}bc)",
            "(?s:(ab)?c)",
            "(?s:(ab)*c)",
            r"(?s:\Qa\E*b)",
            r"(?s:\x41*b)",
            "(?s)a{2,3}",
        ] {
            assert_eq!(strategy(p), S::NeverMatchArm, "{p:?}");
        }
    }

    /// Fix round 4: the predicate is ATTACKED, not enumerated. A seeded
    /// generator over optional-izing constructs; whenever the gate
    /// claims a required leading literal for a compilable pattern, the
    /// compiled pattern must match NO subject lacking that character —
    /// including the empty subject and the pattern's own text with the
    /// character deleted. Every shape the round-4 review found
    /// (`(?s:(a|.*)b)`, `(?s:a?bc)`, `(?s:(ab)?c)`, `(?s:\Qa\E*b)`, …)
    /// falsifies exactly this property under the round-3 predicate.
    #[test]
    fn a_claimed_leading_literal_is_genuinely_required() {
        const TOKENS: &[&str] = &[
            "a", "b", "5", "(", ")", "(?:", "(?s:", "(?m:", "|", "?", "*", "+", "{0}", "{2}",
            "{0,3}", ".", ".*", "[0-9]", "[^a]", r"\Q", r"\E", r"\Qa\E", r"\d", r"\.", r"\x41",
            "^", "$",
        ];
        fn splitmix64(mut x: u64) -> u64 {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        let mut state = 0x0000_0000_0000_0331u64;
        let mut next = || {
            state = state.wrapping_add(1);
            splitmix64(state)
        };
        let mut claimed = 0usize;
        for _ in 0..20_000 {
            let n = next() % 6 + 1;
            let mut p = String::new();
            for _ in 0..n {
                p.push_str(TOKENS[(next() % TOKENS.len() as u64) as usize]);
            }
            let cs: Vec<char> = p.chars().collect();
            if !super::starts_with_required_literal(&cs) {
                continue;
            }
            let first = super::leading_literal_char(&cs).expect("gate passed");
            let Ok(re) = regex::Regex::new(&p) else {
                continue;
            };
            claimed += 1;
            let stripped: String = p.chars().filter(|&c| c != first).collect();
            for subject in [
                "",
                "b",
                "zb",
                "5b",
                "xy",
                "099",
                "()",
                "?",
                "bcd",
                stripped.as_str(),
            ] {
                if subject.contains(first) {
                    continue;
                }
                assert!(
                    !re.is_match(subject),
                    "{p:?}: the gate claimed a required leading {first:?}, but the pattern \\
                     matches {subject:?} which lacks it"
                );
            }
        }
        assert!(claimed > 300, "the attack went vacuous: {claimed} claims");
    }

    /// …while anchors, group openers and literal-denoting escapes ahead
    /// of the first literal keep the rewrite.
    #[test]
    fn literal_leading_shapes_keep_the_rewrite() {
        for (p, want) in [
            ("^(?s:ab)", "^(?s-i:ab)"),
            (r"\A(?s:ab)", r"\A(?s-i:ab)"),
            (r"(?s:\.b)", r"(?s-i:\.b)"),
            (r"(?s:Ab)", r"(?s-i:Ab)"),
            ("(?:(?s:ab))", "(?:(?s-i:ab))"),
            ("(?P<n>x)(?s:ab)", "(?P<n>x)(?s-i:ab)"),
            ("(?s:a.*b)", "(?s-i:a.*b)"),
            // A plain group opener is transparent: the first concrete
            // element is still the literal behind it.
            ("(?s:(ab))", "(?s-i:(ab))"),
        ] {
            assert_eq!(rewritten(p), want, "{p:?}");
        }
    }

    /// The rewrite preserves compilability: for every head form the
    /// Rust crate accepts, the rewritten text still compiles — and for
    /// forms it rejects, the pattern is left alone entirely, so no
    /// rejection can be rewritten into an acceptance.
    #[test]
    fn the_rewrite_preserves_compilability() {
        for p in [
            "(?s:ab)",
            "(?m:ab)",
            "(?U:a+b)",
            "(?smU:ab)",
            "(?-s)ab",
            "(?s)a{2,3}",
            "x(?s:ab)y",
            "(?s:a(?m:b))",
            "(?s:a)|(?m:b)",
        ] {
            // The routing-independent seam: compilability of the
            // rewrite text must hold for every no-i pattern whichever
            // remedy the shape gate picks.
            let out = clickhouse_match_head_rewrite(p).expect("no-i affected pattern");
            assert!(
                regex::Regex::new(&format!("^(?:{p})$")).is_ok(),
                "premise: {p:?} compiles"
            );
            assert!(
                regex::Regex::new(&format!("^(?:{out})$")).is_ok(),
                "{p:?} -> {out:?} must still compile"
            );
        }
        // An invalid pattern carrying a would-be head stays invalid:
        // the head is rewritten but the defect it carries is untouched.
        let out = clickhouse_match_head_rewrite("(?s:ab").expect("no-i affected pattern");
        assert_eq!(out, "(?s-i:ab");
        assert!(regex::Regex::new(&format!("^(?:{out})$")).is_err());
    }

    /// The rewrite is the IDENTITY on any pattern none of its rules
    /// touch: it must never perturb bytes it has no reason to change.
    /// That is a property of THIS function and not a claim about the
    /// engines — `\Qa*\E` below passes through because no rule applies to
    /// it, and docs/api.md §9.4 records that the two engines disagree
    /// about it.
    #[test]
    fn agreed_syntax_passes_through_verbatim() {
        for pattern in [
            r"a\.b(?i)ünïcode.*\n\x7f$1",
            r"\A\afoo\z",
            r"(?:a|b)c",
            r"[^a-z0-9_]",
            r"[a\]b]",
            r"\Qa*\E",
        ] {
            assert_eq!(re2_pattern_to_rust(pattern), pattern, "{pattern:?}");
        }
    }
}
