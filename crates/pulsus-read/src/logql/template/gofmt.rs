//! Go `fmt` port for the template value model (issue #230, plan v1
//! Wave 2 + v7 §A/§B): the verb engine behind `printf`/`print`/
//! `println` and the default `{{ … }}` write path. The domain is the
//! union of every non-default `case` label in `fmt`'s verb dispatch
//! (plan v7 §A): `v t d b o O x X c q U g G e E f F s p T w`, the
//! `%%` literal, and the catch-all `%!<rune>(<type>=<value>)` class.
//!
//! Mirrored mechanics (all `/usr/local/go/src/fmt/print.go` +
//! `format.go`, cross-checked against the go1.26.5-built reference
//! container):
//! - `handleMethods`: Stringer for `v s x X q` (`+v` included),
//!   GoStringer for `#v` only; suppressed while `erroring` (plan v7 R4 —
//!   `badVerb`'s `%v` re-print walks the struct, it never re-consults
//!   `String()`).
//! - `printValue` recursion with the top-level-only pointer dereference
//!   (`&{…}`), depth>0 pointers through `fmtPointer`.
//! - **Pinned-address substitute**: where the reference prints a heap
//!   address (`%p`, a non-nil `Time.loc`/`Location.cacheZone` under a
//!   numeric verb), PulsusDB prints the address value `0x1` — the
//!   reference's own rendering is non-reproducible by construction
//!   (plan v7 §D; ledgered, excluded from corpus goldens).
//! - `%d`-of-`time.Time` struct dumps use Go's real internal layout
//!   (`wall` = nsec for wall-clock-only times, `ext` = seconds since
//!   Jan 1 year 1), so every deterministic cell is byte-exact.

use std::borrow::Cow;

use super::timefns::TemplateEnv;
use super::value::{GoLoc, Value};

/// What the printer needs from the render context: the execution
/// environment (zone resolution for `time` Stringers) and the label map
/// (only materialised if a `LabelMap` value is actually printed).
pub trait PrintEnv {
    fn env(&self) -> &TemplateEnv;
    /// Sorted `(name, value)` pairs of the template data map, including
    /// the error pair when the caller's visibility gate is open.
    fn label_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// The per-render output budget (issue #230 follow-up): padding
    /// widths/precisions are caller-multiplied allocations and charge
    /// here BEFORE writing. On breach the printer stops emitting (the
    /// evaluator aborts the query; partial output is never observable).
    fn budget(&self) -> &super::RenderBudget;
}

// ---------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------

/// The template action write path (`exec.go printValue` over
/// `printableValue`): `%v` semantics except that the invalid value
/// renders `<no value>`.
pub fn write_template_value(out: &mut Vec<u8>, v: &Value<'_>, env: &dyn PrintEnv) {
    if matches!(v, Value::Nil) {
        out.extend_from_slice(b"<no value>");
        return;
    }
    let mut p = P::new(out, env);
    p.print_arg(v, 'v');
}

/// `fmt.Sprintf` over the value model.
pub fn sprintf(out: &mut Vec<u8>, format: &[u8], args: &[Value<'_>], env: &dyn PrintEnv) {
    let mut p = P::new(out, env);
    p.do_printf(format, args);
}

/// `fmt.Sprint`: spaces between operands when neither is a string.
pub fn sprint(out: &mut Vec<u8>, args: &[Value<'_>], env: &dyn PrintEnv) {
    let mut p = P::new(out, env);
    let mut prev_string = false;
    for (i, arg) in args.iter().enumerate() {
        let is_string = matches!(arg, Value::Str(_));
        if i > 0 && !is_string && !prev_string {
            p.out.push(b' ');
        }
        p.print_arg(arg, 'v');
        prev_string = is_string;
    }
}

/// `fmt.Sprintln`.
pub fn sprintln(out: &mut Vec<u8>, args: &[Value<'_>], env: &dyn PrintEnv) {
    let mut p = P::new(out, env);
    for (i, arg) in args.iter().enumerate() {
        if i > 0 {
            p.out.push(b' ');
        }
        p.print_arg(arg, 'v');
    }
    p.out.push(b'\n');
}

/// The pinned deterministic stand-in for a heap address the reference
/// would print (plan v7 §D: excluded cells; AC-20 shape contract). The
/// value is deliberately DISTINCTIVE so tests (and the AC-22 audit's
/// address-token scan) can prove no corpus golden ever contains a
/// pinned-address rendering: hex `0xfa11ed`, decimal `16388589`.
pub const PINNED_ADDR: u64 = 0xFA11ED;

// ---------------------------------------------------------------------
// Formatter state
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
struct Flags {
    wid: usize,
    wid_present: bool,
    prec: usize,
    prec_present: bool,
    minus: bool,
    plus: bool,
    sharp: bool,
    space: bool,
    zero: bool,
    plus_v: bool,
    sharp_v: bool,
}

/// What `fmt_pointer`'s badVerb `%v` re-print shows for a NON-nil
/// pointer.
enum Reprint<'p, 'a> {
    /// The pinned address (an opaque pointee).
    Addr,
    /// Top-level dereference of the pointee (`&{…}`).
    Deref(&'p Value<'a>),
}

struct P<'o, 'e> {
    out: &'o mut Vec<u8>,
    f: Flags,
    erroring: bool,
    reordered: bool,
    good_arg_num: bool,
    env: &'e dyn PrintEnv,
}

impl<'o, 'e> P<'o, 'e> {
    fn new(out: &'o mut Vec<u8>, env: &'e dyn PrintEnv) -> Self {
        P {
            out,
            f: Flags::default(),
            erroring: false,
            reordered: false,
            good_arg_num: true,
            env,
        }
    }

    // -- padding ------------------------------------------------------

    fn write_padding(&mut self, n: usize) {
        // Caller-multiplied allocation: charge before writing (issue
        // #230 follow-up). On breach the budget is poisoned and the
        // render aborts the query — nothing partial is observable.
        if self.env.budget().charge(n).is_err() {
            return;
        }
        let pad = if self.f.zero { b'0' } else { b' ' };
        self.out.extend(std::iter::repeat_n(pad, n));
    }

    /// `fmt.pad`: rune-count-aware width padding.
    fn pad(&mut self, b: &[u8]) {
        if !self.f.wid_present || self.f.wid == 0 {
            self.out.extend_from_slice(b);
            return;
        }
        let runes = rune_count(b);
        if self.f.wid <= runes {
            self.out.extend_from_slice(b);
            return;
        }
        let width = self.f.wid - runes;
        if self.f.minus {
            self.out.extend_from_slice(b);
            let zero = self.f.zero;
            self.f.zero = false;
            self.write_padding(width);
            self.f.zero = zero;
        } else {
            self.write_padding(width);
            self.out.extend_from_slice(b);
        }
    }

    fn pad_str(&mut self, s: &[u8]) {
        // padString always pads with spaces.
        let zero = self.f.zero;
        self.f.zero = false;
        self.pad(s);
        self.f.zero = zero;
    }

    // -- error forms ----------------------------------------------------

    /// `pp.badVerb` — including the R4 `erroring` suppression of
    /// Stringer during the `%v` re-print.
    fn bad_verb(&mut self, verb: char, v: Option<&Value<'_>>) {
        self.erroring = true;
        self.out.extend_from_slice(b"%!");
        push_char(self.out, verb);
        self.out.push(b'(');
        match v {
            Some(Value::Nil) | None => self.out.extend_from_slice(b"<nil>"),
            Some(val) => {
                self.out.extend_from_slice(val.type_name().as_bytes());
                self.out.push(b'=');
                let saved = self.f;
                self.f = Flags::default();
                self.print_arg(val, 'v');
                self.f = saved;
            }
        }
        self.out.push(b')');
        // Go clears the flag outright (print.go:399) — a nested badVerb
        // inside a re-print re-enables Stringer for the remainder, and
        // the port mirrors that exactly.
        self.erroring = false;
    }

    fn missing_arg(&mut self, verb: char) {
        self.out.extend_from_slice(b"%!");
        push_char(self.out, verb);
        self.out.extend_from_slice(b"(MISSING)");
    }

    fn bad_arg_num(&mut self, verb: char) {
        self.out.extend_from_slice(b"%!");
        push_char(self.out, verb);
        self.out.extend_from_slice(b"(BADINDEX)");
    }

    // -- integers -------------------------------------------------------

    /// `fmt.fmtInteger` (format.go).
    fn fmt_integer(&mut self, u: u64, base: u32, is_signed: bool, verb: char, upper: bool) {
        let negative = is_signed && (u as i64) < 0;
        let mag = if negative {
            (u as i64).unsigned_abs()
        } else {
            u
        };

        let mut prec = 0usize;
        if self.f.prec_present {
            prec = self.f.prec;
            if prec == 0 && mag == 0 {
                let zero = self.f.zero;
                self.f.zero = false;
                if self.f.wid_present {
                    let w = self.f.wid;
                    self.write_padding(w);
                }
                self.f.zero = zero;
                return;
            }
        } else if self.f.zero && !self.f.minus && self.f.wid_present {
            prec = self.f.wid;
            if negative || self.f.plus || self.f.space {
                prec = prec.saturating_sub(1);
            }
        }

        let digits: &[u8] = if upper {
            b"0123456789ABCDEF"
        } else {
            b"0123456789abcdef"
        };
        // Go grows the buffer to `3 + wid + prec` when the fixed one is
        // too small (format.go); the growth is caller-multiplied, so it
        // charges the render budget first.
        let needed = 8 + prec.max(if self.f.wid_present { self.f.wid } else { 0 });
        let mut heap_buf;
        let mut stack_buf = [0u8; 96];
        let buf: &mut [u8] = if needed > 96 {
            if self.env.budget().charge(needed).is_err() {
                return;
            }
            heap_buf = vec![0u8; needed];
            &mut heap_buf
        } else {
            &mut stack_buf
        };
        let mut i = buf.len();
        let mut u = mag;
        loop {
            i -= 1;
            buf[i] = digits[(u % base as u64) as usize];
            u /= base as u64;
            if u == 0 {
                break;
            }
        }
        while buf.len() - i < prec && i > 0 {
            i -= 1;
            buf[i] = b'0';
        }
        if self.f.sharp {
            match base {
                2 => {
                    i -= 1;
                    buf[i] = b'b';
                    i -= 1;
                    buf[i] = b'0';
                }
                8 => {
                    if buf[i] != b'0' {
                        i -= 1;
                        buf[i] = b'0';
                    }
                }
                16 => {
                    i -= 1;
                    buf[i] = if upper { b'X' } else { b'x' };
                    i -= 1;
                    buf[i] = b'0';
                }
                _ => {}
            }
        }
        if verb == 'O' {
            i -= 1;
            buf[i] = b'o';
            i -= 1;
            buf[i] = b'0';
        }
        if negative {
            i -= 1;
            buf[i] = b'-';
        } else if self.f.plus {
            i -= 1;
            buf[i] = b'+';
        } else if self.f.space {
            i -= 1;
            buf[i] = b' ';
        }
        let zero = self.f.zero;
        self.f.zero = false;
        let owned = buf[i..].to_vec();
        self.pad(&owned);
        self.f.zero = zero;
    }

    /// Charges a caller-multiplied allocation; `false` = breached (the
    /// printer skips the write; the evaluator aborts the query).
    fn charge(&mut self, n: usize) -> bool {
        self.env.budget().charge(n).is_ok()
    }

    /// `pp.fmtInteger` verb dispatch.
    fn dispatch_integer(&mut self, u: u64, is_signed: bool, verb: char, v: &Value<'_>) {
        match verb {
            'v' => {
                if self.f.sharp_v && !is_signed {
                    self.fmt0x64(u, true);
                } else {
                    self.fmt_integer(u, 10, is_signed, verb, false);
                }
            }
            'd' => self.fmt_integer(u, 10, is_signed, verb, false),
            'b' => self.fmt_integer(u, 2, is_signed, verb, false),
            'o' | 'O' => self.fmt_integer(u, 8, is_signed, verb, false),
            'x' => self.fmt_integer(u, 16, is_signed, verb, false),
            'X' => self.fmt_integer(u, 16, is_signed, verb, true),
            'c' => self.fmt_c(u),
            'q' => self.fmt_qc(u),
            'U' => self.fmt_unicode(u),
            _ => self.bad_verb(verb, Some(v)),
        }
    }

    fn fmt0x64(&mut self, v: u64, leading_0x: bool) {
        let sharp = self.f.sharp;
        self.f.sharp = leading_0x;
        self.fmt_integer(v, 16, false, 'v', false);
        self.f.sharp = sharp;
    }

    /// `fmt.fmtC`: the rune.
    fn fmt_c(&mut self, c: u64) {
        let r = u32::try_from(c)
            .ok()
            .and_then(char::from_u32)
            .unwrap_or('\u{FFFD}');
        let mut buf = [0u8; 4];
        let enc = r.encode_utf8(&mut buf).as_bytes().to_vec();
        self.pad(&enc);
    }

    /// `fmt.fmtQc`: quoted rune.
    fn fmt_qc(&mut self, c: u64) {
        let r = u32::try_from(c)
            .ok()
            .and_then(char::from_u32)
            .unwrap_or('\u{FFFD}');
        let quoted = quote_rune(r, self.f.plus);
        let owned = quoted.into_bytes();
        self.pad(&owned);
    }

    /// `fmt.fmtUnicode`: `U+0078` (+ ` 'x'` under `#`).
    fn fmt_unicode(&mut self, u: u64) {
        let mut prec = 4usize;
        if self.f.prec_present && self.f.prec > 4 {
            prec = self.f.prec;
        }
        if !self.charge(prec + 8) {
            return;
        }
        let mut s = format!("U+{u:0width$X}", width = prec);
        if self.f.sharp
            && u <= 0x10FFFF
            && let Some(r) = char::from_u32(u as u32)
            && is_print(r)
        {
            s.push_str(" '");
            s.push(r);
            s.push('\'');
        }
        let owned = s.into_bytes();
        self.pad(&owned);
    }

    // -- floats ---------------------------------------------------------

    /// `pp.fmtFloat` + `fmt.fmtFloat` (default precisions, sign
    /// handling, `#` alternate form, Inf/NaN padding rules).
    fn dispatch_float(&mut self, val: f64, verb: char, v: &Value<'_>) {
        let (conv_verb, default_prec): (char, i32) = match verb {
            'v' => ('g', -1),
            'b' | 'g' | 'G' | 'x' | 'X' => (verb, -1),
            'f' | 'e' | 'E' => (verb, 6),
            'F' => ('f', 6),
            _ => {
                self.bad_verb(verb, Some(v));
                return;
            }
        };
        let prec = if self.f.prec_present {
            self.f.prec as i32
        } else {
            default_prec
        };
        let mut num = format_float_go(val, conv_verb, prec);
        // Ensure a leading sign byte.
        if num.first() != Some(&b'-') {
            num.insert(0, b'+');
        }
        if self.f.space && num[0] == b'+' && !self.f.plus {
            num[0] = b' ';
        }
        // Inf/NaN: never zero-padded.
        if num[1] == b'I' || num[1] == b'N' {
            let zero = self.f.zero;
            self.f.zero = false;
            if num[1] == b'N' && !self.f.space && !self.f.plus {
                num.remove(0);
            }
            self.pad(&num);
            self.f.zero = zero;
            return;
        }
        // Sharp: force decimal point, restore trailing zeros.
        if self.f.sharp && verb != 'b' {
            let mut digits: i32 = match verb {
                'v' => 6,
                'g' | 'G' | 'x' | 'X' => -1,
                'e' | 'E' => 6,
                'f' | 'F' => 6,
                _ => 0,
            };
            if self.f.prec_present {
                digits = self.f.prec as i32;
            }
            let mut tail: Vec<u8> = Vec::new();
            let mut has_point = false;
            let mut saw_nonzero = false;
            let mut i = 1;
            while i < num.len() {
                match num[i] {
                    b'.' => has_point = true,
                    b'p' | b'P' => {
                        tail.extend_from_slice(&num[i..]);
                        num.truncate(i);
                        break;
                    }
                    b'e' | b'E' if verb != 'x' && verb != 'X' => {
                        tail.extend_from_slice(&num[i..]);
                        num.truncate(i);
                        break;
                    }
                    c => {
                        if c != b'0' {
                            saw_nonzero = true;
                        }
                        if saw_nonzero {
                            digits -= 1;
                        }
                    }
                }
                i += 1;
            }
            if !has_point {
                if num.len() == 2 && num[1] == b'0' {
                    digits -= 1;
                }
                num.push(b'.');
            }
            while digits > 0 {
                num.push(b'0');
                digits -= 1;
            }
            num.extend_from_slice(&tail);
        }
        if self.f.plus || num[0] != b'+' {
            if self.f.zero && self.f.wid_present && self.f.wid > num.len() {
                self.out.push(num[0]);
                let w = self.f.wid - num.len();
                self.write_padding(w);
                self.out.extend_from_slice(&num[1..]);
                return;
            }
            self.pad(&num);
            return;
        }
        let unsigned = num[1..].to_vec();
        self.pad(&unsigned);
    }

    /// `pp.fmtComplex`: `(r+ji)`.
    fn dispatch_complex(&mut self, re: f64, im: f64, verb: char, v: &Value<'_>) {
        match verb {
            'v' | 'b' | 'g' | 'G' | 'x' | 'X' | 'f' | 'F' | 'e' | 'E' => {
                let old_plus = self.f.plus;
                self.out.push(b'(');
                self.dispatch_float(re, verb, v);
                self.f.plus = true;
                self.dispatch_float(im, verb, v);
                self.out.extend_from_slice(b"i)");
                self.f.plus = old_plus;
            }
            _ => self.bad_verb(verb, Some(v)),
        }
    }

    // -- strings ----------------------------------------------------------

    fn fmt_s(&mut self, s: &[u8]) {
        let end = truncate_len(s, self.f.prec_present.then_some(self.f.prec));
        self.pad_str(&s[..end]);
    }

    fn fmt_q(&mut self, s: &[u8]) {
        let prec = self.f.prec_present.then_some(self.f.prec);
        let t = &s[..truncate_len(s, prec)];
        // Quoting expands at most 10× per rune (`\U censored` form).
        if !self.charge(t.len().saturating_mul(10) + 2) {
            return;
        }
        if self.f.sharp && can_backquote(t) {
            let mut b = Vec::with_capacity(t.len() + 2);
            b.push(b'`');
            b.extend_from_slice(t);
            b.push(b'`');
            self.pad(&b);
            return;
        }
        let quoted = if self.f.plus {
            quote_bytes_ascii(t)
        } else {
            quote_bytes(t, '"')
        };
        let owned = quoted.into_bytes();
        self.pad(&owned);
    }

    /// `fmt.fmtSbx`: hex dump of a string/byte slice.
    fn fmt_sbx(&mut self, s: &[u8], upper: bool) {
        let digits: &[u8] = if upper {
            b"0123456789ABCDEF"
        } else {
            b"0123456789abcdef"
        };
        let mut length = s.len();
        if self.f.prec_present && self.f.prec < length {
            length = self.f.prec;
        }
        let mut width = 2 * length;
        if width > 0 {
            if self.f.space {
                if self.f.sharp {
                    width *= 2;
                }
                width += length - 1;
            } else if self.f.sharp {
                width += 2;
            }
        } else {
            if self.f.wid_present {
                let w = self.f.wid;
                self.write_padding(w);
            }
            return;
        }
        if self.f.wid_present && self.f.wid > width && !self.f.minus {
            let w = self.f.wid - width;
            self.write_padding(w);
        }
        for (i, &c) in s[..length].iter().enumerate() {
            if self.f.space {
                if i > 0 {
                    self.out.push(b' ');
                }
                if self.f.sharp {
                    self.out.push(b'0');
                    self.out.push(if upper { b'X' } else { b'x' });
                }
            } else if self.f.sharp && i == 0 {
                self.out.push(b'0');
                self.out.push(if upper { b'X' } else { b'x' });
            }
            self.out.push(digits[(c >> 4) as usize]);
            self.out.push(digits[(c & 0xF) as usize]);
        }
        if self.f.wid_present && self.f.wid > width && self.f.minus {
            let w = self.f.wid - width;
            self.write_padding(w);
        }
    }

    fn dispatch_string(&mut self, s: &[u8], verb: char, v: &Value<'_>) {
        match verb {
            'v' => {
                if self.f.sharp_v {
                    self.fmt_q(s);
                } else {
                    self.fmt_s(s);
                }
            }
            's' => self.fmt_s(s),
            'x' => self.fmt_sbx(s, false),
            'X' => self.fmt_sbx(s, true),
            'q' => self.fmt_q(s),
            _ => self.bad_verb(verb, Some(v)),
        }
    }

    /// `pp.fmtBytes` — `type_string` is `[]byte` at the top level and
    /// `[]uint8` through reflection (Go passes the literal).
    fn dispatch_bytes(&mut self, b: &[u8], verb: char, type_string: &str, _v: &Value<'_>) {
        match verb {
            'v' | 'd' => {
                if self.f.sharp_v {
                    self.out.extend_from_slice(type_string.as_bytes());
                    self.out.push(b'{');
                    for (i, &c) in b.iter().enumerate() {
                        if i > 0 {
                            self.out.extend_from_slice(b", ");
                        }
                        self.fmt0x64(c as u64, true);
                    }
                    self.out.push(b'}');
                } else {
                    self.out.push(b'[');
                    for (i, &c) in b.iter().enumerate() {
                        if i > 0 {
                            self.out.push(b' ');
                        }
                        self.fmt_integer(c as u64, 10, false, verb, false);
                    }
                    self.out.push(b']');
                }
            }
            's' => self.fmt_s(b),
            'x' => self.fmt_sbx(b, false),
            'X' => self.fmt_sbx(b, true),
            'q' => self.fmt_q(b),
            _ => {
                // Fall to the reflect slice walk: per-element dispatch.
                self.print_byte_slice_walk(b, verb);
            }
        }
    }

    fn print_byte_slice_walk(&mut self, b: &[u8], verb: char) {
        if self.f.sharp_v {
            self.out.extend_from_slice(b"[]uint8{");
            for (i, &c) in b.iter().enumerate() {
                if i > 0 {
                    self.out.extend_from_slice(b", ");
                }
                let elem = Value::Uint(c as u64, super::value::UintKind::Uint8);
                self.print_value(&elem, verb, 1);
            }
            self.out.push(b'}');
        } else {
            self.out.push(b'[');
            for (i, &c) in b.iter().enumerate() {
                if i > 0 {
                    self.out.push(b' ');
                }
                let elem = Value::Uint(c as u64, super::value::UintKind::Uint8);
                self.print_value(&elem, verb, 1);
            }
            self.out.push(b']');
        }
    }

    // -- booleans ---------------------------------------------------------

    fn dispatch_bool(&mut self, b: bool, verb: char, v: &Value<'_>) {
        match verb {
            't' | 'v' => {
                let s: &[u8] = if b { b"true" } else { b"false" };
                let owned = s.to_vec();
                self.pad(&owned);
            }
            _ => self.bad_verb(verb, Some(v)),
        }
    }

    // -- pointers -----------------------------------------------------------

    /// `pp.fmtPointer` with the pinned address stand-in. `addr` is
    /// `None` for a nil pointer; `type_name` is the pointer TYPE's
    /// rendering (`*time.Location` / `*time.zone`); `reprint` decides
    /// what the badVerb `%v` re-print shows (a nil pointer → `<nil>`, a
    /// dereferenceable one → the `&{…}` walk, an opaque one → the
    /// pinned address).
    fn fmt_pointer(
        &mut self,
        addr: Option<u64>,
        verb: char,
        type_name: &str,
        reprint: Reprint<'_, '_>,
    ) {
        let u = addr.unwrap_or(0);
        match verb {
            'v' => {
                if self.f.sharp_v {
                    self.out.push(b'(');
                    self.out.extend_from_slice(type_name.as_bytes());
                    self.out.extend_from_slice(b")(");
                    if u == 0 {
                        self.out.extend_from_slice(b"nil");
                    } else {
                        self.fmt0x64(u, true);
                    }
                    self.out.push(b')');
                } else if u == 0 {
                    self.pad_str(b"<nil>");
                } else {
                    self.fmt0x64(u, !self.f.sharp);
                }
            }
            'p' => self.fmt0x64(u, !self.f.sharp),
            'b' => self.fmt_integer(u, 2, false, verb, false),
            'o' => self.fmt_integer(u, 8, false, verb, false),
            'd' => self.fmt_integer(u, 10, false, verb, false),
            'x' => self.fmt_integer(u, 16, false, verb, false),
            'X' => self.fmt_integer(u, 16, false, verb, true),
            _ => {
                // badVerb: `%!<verb>(<type>=<%v re-print>)` with
                // Stringer suppressed (R4).
                self.erroring = true;
                self.out.extend_from_slice(b"%!");
                push_char(self.out, verb);
                self.out.push(b'(');
                self.out.extend_from_slice(type_name.as_bytes());
                self.out.push(b'=');
                match reprint {
                    _ if u == 0 => self.out.extend_from_slice(b"<nil>"),
                    Reprint::Deref(v) => {
                        let saved = self.f;
                        self.f = Flags::default();
                        self.print_value(v, 'v', 0);
                        self.f = saved;
                    }
                    Reprint::Addr => self.fmt0x64(u, true),
                }
                self.out.push(b')');
                self.erroring = false;
            }
        }
    }

    // -- the argument printer ------------------------------------------------

    /// `pp.printArg`.
    fn print_arg(&mut self, arg: &Value<'_>, verb: char) {
        if matches!(arg, Value::Nil) {
            match verb {
                'T' | 'v' => self.pad_str(b"<nil>"),
                _ => self.bad_verb(verb, None),
            }
            return;
        }
        match verb {
            'T' => {
                let name = arg.type_name().to_string();
                self.fmt_s(name.as_bytes());
                return;
            }
            'p' => {
                match arg {
                    // Pointer/slice/map kinds carry an address — the
                    // pinned substitute (plan v7 §D items 1/3/4).
                    Value::Bytes(_)
                    | Value::List(_)
                    | Value::Map(_)
                    | Value::LabelMap
                    | Value::Location(_) => {
                        self.fmt_pointer(Some(PINNED_ADDR), 'p', arg.type_name(), Reprint::Addr);
                    }
                    _ => self.bad_verb('p', Some(arg)),
                }
                return;
            }
            _ => {}
        }
        match arg {
            Value::Bool(b) => self.dispatch_bool(*b, verb, arg),
            Value::Float(f) => self.dispatch_float(*f, verb, arg),
            Value::Complex(re, im) => self.dispatch_complex(*re, *im, verb, arg),
            Value::Int(n, _) => self.dispatch_integer(*n as u64, true, verb, arg),
            Value::Uint(n, _) => self.dispatch_integer(*n, false, verb, arg),
            Value::Str(s) => self.dispatch_string(s, verb, arg),
            Value::Bytes(b) => self.dispatch_bytes(b, verb, "[]byte", arg),
            _ => {
                if !self.handle_methods(arg, verb) {
                    self.print_value(arg, verb, 0);
                }
            }
        }
    }

    /// `pp.handleMethods` — Stringer/GoStringer interception with the
    /// R4 `erroring` suppression. `%w` on a non-error is a badVerb.
    fn handle_methods(&mut self, arg: &Value<'_>, verb: char) -> bool {
        if self.erroring {
            return false;
        }
        if verb == 'w' {
            // None of the model's values implement `error`.
            self.bad_verb('w', Some(arg));
            return true;
        }
        if self.f.sharp_v {
            // GoStringer: only time.Time.
            if let Value::Time(t) = arg {
                let s = t.go_string(self.env.env());
                self.fmt_s(s.as_bytes());
                return true;
            }
            return false;
        }
        if matches!(verb, 'v' | 's' | 'x' | 'X' | 'q') {
            let text: Option<String> = match arg {
                Value::Time(t) => Some(t.string(self.env.env())),
                Value::Duration(d) => Some(super::timefns::duration_string(*d)),
                Value::Month(m) => Some(super::timefns::month_string(*m)),
                Value::Weekday(w) => Some(super::timefns::weekday_string(*w)),
                Value::Location(l) => Some(super::timefns::location_name(l, self.env.env())),
                _ => None,
            };
            if let Some(s) = text {
                self.dispatch_string(s.as_bytes(), verb, arg);
                return true;
            }
        }
        false
    }

    /// `pp.printValue` — the reflect walk. Scalars re-dispatch; the
    /// composite kinds (maps, slices, the two `time` structs, pointers)
    /// recurse with Go's separators and the top-level-only pointer
    /// dereference.
    fn print_value(&mut self, value: &Value<'_>, verb: char, depth: usize) {
        if depth > 0 && self.handle_methods(value, verb) {
            return;
        }
        match value {
            Value::Nil => {
                if depth == 0 {
                    self.out.extend_from_slice(b"<invalid reflect.Value>");
                } else if self.f.sharp_v {
                    // Interface-kind element (`printValue` Interface
                    // case): the nil elem prints the type + (nil).
                    self.out.extend_from_slice(b"interface {}(nil)");
                } else {
                    // The Interface case writes `<nil>` REGARDLESS of
                    // verb — a nil elem never badVerbs.
                    self.out.extend_from_slice(b"<nil>");
                }
            }
            Value::Bool(b) => self.dispatch_bool(*b, verb, value),
            Value::Int(n, _) => self.dispatch_integer(*n as u64, true, verb, value),
            Value::Uint(n, _) => self.dispatch_integer(*n, false, verb, value),
            Value::Float(f) => self.dispatch_float(*f, verb, value),
            Value::Complex(re, im) => self.dispatch_complex(*re, *im, verb, value),
            Value::Str(s) => self.dispatch_string(s, verb, value),
            // Through reflection the type reads []uint8; Go reaches
            // Bytes at depth 0 only via printArg, which already
            // dispatched — keep dispatch_bytes for both.
            Value::Bytes(b) => self.dispatch_bytes(b, verb, "[]uint8", value),
            Value::List(items) => {
                if self.f.sharp_v {
                    self.out.extend_from_slice(b"[]interface {}{");
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            self.out.extend_from_slice(b", ");
                        }
                        if matches!(item, Value::Nil) {
                            // interface elem, nil: type + (nil)
                            self.out.extend_from_slice(b"interface {}(nil)");
                        } else {
                            self.print_value(item, verb, depth + 1);
                        }
                    }
                    self.out.push(b'}');
                } else {
                    self.out.push(b'[');
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            self.out.push(b' ');
                        }
                        self.print_value(item, verb, depth + 1);
                    }
                    self.out.push(b']');
                }
            }
            Value::Map(entries) => {
                let owned: Vec<(Vec<u8>, Value<'_>)> = entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                self.print_map(&owned, "map[string]interface {}", true, verb, depth);
            }
            Value::LabelMap => {
                let pairs = self.env.label_pairs();
                let owned: Vec<(Vec<u8>, Value<'_>)> = pairs
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(Cow::Owned(v))))
                    .collect();
                self.print_map(&owned, "map[string]string", false, verb, depth);
            }
            Value::Time(t) => {
                // struct { wall uint64; ext int64; loc *Location }
                let env = self.env.env();
                let (wall, ext) = t.internal_repr();
                self.print_struct_open("time.Time");
                self.print_field_name("wall");
                let wall_v = Value::Uint(wall, super::value::UintKind::Uint64);
                self.print_value(&wall_v, verb, depth + 1);
                self.field_sep();
                self.print_field_name("ext");
                let ext_v = Value::int64(ext);
                self.print_value(&ext_v, verb, depth + 1);
                self.field_sep();
                self.print_field_name("loc");
                match t.loc_pointer(env) {
                    None => {
                        // nil *Location
                        self.fmt_pointer(None, verb, "*time.Location", Reprint::Addr);
                    }
                    Some(loc) => {
                        // Non-nil: a numeric verb prints the ADDRESS —
                        // the pinned substitute (§D items 2/5); a
                        // badVerb re-print dereferences (&{…}, R3).
                        let loc_val = Value::Location(loc);
                        self.fmt_pointer(
                            Some(PINNED_ADDR),
                            verb,
                            "*time.Location",
                            Reprint::Deref(&loc_val),
                        );
                    }
                }
                self.out.push(b'}');
            }
            Value::Location(loc) => {
                // *time.Location: top-level pointer dereference (&{…});
                // at depth > 0 it prints as an address.
                if depth == 0 {
                    self.out.push(b'&');
                    self.print_location_struct(&loc.clone(), verb, depth + 1);
                } else {
                    self.fmt_pointer(
                        Some(PINNED_ADDR),
                        verb,
                        "*time.Location",
                        Reprint::Deref(value),
                    );
                }
            }
            Value::Duration(n) => self.dispatch_integer(*n as u64, true, verb, value),
            Value::Month(n) => self.dispatch_integer(*n as u64, true, verb, value),
            Value::Weekday(n) => self.dispatch_integer(*n as u64, true, verb, value),
        }
    }

    fn field_sep(&mut self) {
        if self.f.sharp_v {
            self.out.extend_from_slice(b", ");
        } else {
            self.out.push(b' ');
        }
    }

    fn print_struct_open(&mut self, type_name: &str) {
        if self.f.sharp_v {
            self.out.extend_from_slice(type_name.as_bytes());
        }
        self.out.push(b'{');
    }

    fn print_field_name(&mut self, name: &str) {
        if self.f.plus_v || self.f.sharp_v {
            self.out.extend_from_slice(name.as_bytes());
            self.out.push(b':');
        }
    }

    /// The `time.Location` struct body: `{name zone tx extend
    /// cacheStart cacheEnd cacheZone}`. The stock (never-looked-up)
    /// UTC/Local form has nil slices and a nil `cacheZone`; a LOADED
    /// zone's slices embed the IANA transition table in the reference —
    /// non-reproducible + tzdata-coupled, so PulsusDB pins the
    /// deterministic substitute: empty tables + the pinned `cacheZone`
    /// address (ledgered; excluded from corpus goldens).
    fn print_location_struct(&mut self, loc: &GoLoc, verb: char, depth: usize) {
        let env = self.env.env();
        let name = super::timefns::location_struct_name(loc, env);
        let loaded = matches!(loc, GoLoc::Named(_));
        if self.f.sharp_v {
            self.out.extend_from_slice(b"time.Location{");
            self.out.extend_from_slice(b"name:");
            self.fmt_q(name.as_bytes());
            self.out
                .extend_from_slice(b", zone:[]time.zone(nil), tx:[]time.zoneTrans(nil), extend:");
            self.fmt_q(b"");
            self.out
                .extend_from_slice(b", cacheStart:0, cacheEnd:0, cacheZone:(*time.zone)(");
            if loaded {
                self.fmt0x64(PINNED_ADDR, true);
            } else {
                self.out.extend_from_slice(b"nil");
            }
            self.out.extend_from_slice(b")}");
            return;
        }
        self.out.push(b'{');
        // name string
        self.print_field_name("name");
        let name_v = Value::str_owned(name.clone().into_bytes());
        self.print_value(&name_v, verb, depth);
        self.field_sep();
        // zone []zone (pinned empty)
        self.print_field_name("zone");
        self.out.extend_from_slice(b"[]");
        self.field_sep();
        // tx []zoneTrans (pinned empty)
        self.print_field_name("tx");
        self.out.extend_from_slice(b"[]");
        self.field_sep();
        // extend string (empty)
        self.print_field_name("extend");
        let extend_v = Value::str_owned(Vec::new());
        self.print_value(&extend_v, verb, depth);
        self.field_sep();
        self.print_field_name("cacheStart");
        // The pinned substitute zeroes the cache window for BOTH the
        // stock and the loaded form (ledgered — the loaded form's real
        // values are tzdata-coupled).
        let zero = Value::int64(0);
        self.print_value(&zero, verb, depth);
        self.field_sep();
        self.print_field_name("cacheEnd");
        self.print_value(&zero, verb, depth);
        self.field_sep();
        self.print_field_name("cacheZone");
        let addr = if loaded { Some(PINNED_ADDR) } else { None };
        // Non-nil (loaded zone) cacheZone content is tzdata-coupled and
        // non-reproducible in the reference — the pinned substitute
        // shows the pinned address (ledgered; never a corpus golden).
        self.fmt_pointer(addr, verb, "*time.zone", Reprint::Addr);
        self.out.push(b'}');
    }

    fn print_map(
        &mut self,
        entries: &[(Vec<u8>, Value<'_>)],
        type_name: &str,
        _any_elems: bool,
        verb: char,
        depth: usize,
    ) {
        if self.f.sharp_v {
            self.out.extend_from_slice(type_name.as_bytes());
            self.out.push(b'{');
        } else {
            self.out.extend_from_slice(b"map[");
        }
        for (i, (k, v)) in entries.iter().enumerate() {
            if i > 0 {
                if self.f.sharp_v {
                    self.out.extend_from_slice(b", ");
                } else {
                    self.out.push(b' ');
                }
            }
            let key = Value::str_owned(k.clone());
            self.print_value(&key, verb, depth + 1);
            self.out.push(b':');
            self.print_value(v, verb, depth + 1);
        }
        if self.f.sharp_v {
            self.out.push(b'}');
        } else {
            self.out.push(b']');
        }
    }

    // -- doPrintf ---------------------------------------------------------

    /// `pp.doPrintf` — flags, width/precision (incl. `*`), explicit
    /// argument indexes (`%[n]d`), and every malformed form.
    fn do_printf(&mut self, format: &[u8], args: &[Value<'_>]) {
        let end = format.len();
        let mut arg_num = 0usize;
        // Set per verb below; the pre-loop value is never read.
        let mut after_index;
        self.reordered = false;
        let mut i = 0usize;
        'format_loop: while i < end {
            self.good_arg_num = true;
            let last_i = i;
            while i < end && format[i] != b'%' {
                i += 1;
            }
            if i > last_i {
                self.out.extend_from_slice(&format[last_i..i]);
            }
            if i >= end {
                break;
            }
            // Process one verb.
            i += 1;
            self.f = Flags::default();
            let mut simple = true;
            while i < end && simple {
                let c = format[i];
                match c {
                    b'#' => self.f.sharp = true,
                    b'0' => self.f.zero = true,
                    b'+' => self.f.plus = true,
                    b'-' => self.f.minus = true,
                    b' ' => self.f.space = true,
                    _ => {
                        if c.is_ascii_lowercase() && arg_num < args.len() {
                            match c {
                                b'v' | b'w' => {
                                    self.f.sharp_v = self.f.sharp;
                                    self.f.sharp = false;
                                    self.f.plus_v = self.f.plus;
                                    self.f.plus = false;
                                }
                                _ => {}
                            }
                            if c == b'w' {
                                // Outside Errorf every %w is a badVerb
                                // through handleMethods.
                                self.print_arg_w(&args[arg_num]);
                            } else {
                                self.print_arg(&args[arg_num], c as char);
                            }
                            arg_num += 1;
                            i += 1;
                            continue 'format_loop;
                        }
                        simple = false;
                        continue;
                    }
                }
                i += 1;
            }

            // Explicit argument index?
            (arg_num, i, after_index) = self.arg_number(arg_num, format, i, args.len());

            // Width?
            if i < end && format[i] == b'*' {
                i += 1;
                let (w, ok, new_arg) = int_from_arg(args, arg_num);
                arg_num = new_arg;
                self.f.wid_present = ok;
                if !ok {
                    self.out.extend_from_slice(b"%!(BADWIDTH)");
                } else if w < 0 {
                    self.f.wid = (-w) as usize;
                    self.f.minus = true;
                    self.f.zero = false;
                } else {
                    self.f.wid = w as usize;
                }
                after_index = false;
            } else {
                let (w, ok, ni) = parse_num(format, i, end);
                i = ni;
                self.f.wid = w;
                self.f.wid_present = ok;
                if after_index && ok {
                    self.good_arg_num = false;
                }
            }

            // Precision?
            if i + 1 < end && format[i] == b'.' {
                i += 1;
                if after_index {
                    self.good_arg_num = false;
                }
                (arg_num, i, after_index) = self.arg_number(arg_num, format, i, args.len());
                if i < end && format[i] == b'*' {
                    i += 1;
                    let (p, ok, new_arg) = int_from_arg(args, arg_num);
                    arg_num = new_arg;
                    if p < 0 {
                        self.f.prec = 0;
                        self.f.prec_present = false;
                    } else {
                        self.f.prec = p as usize;
                        self.f.prec_present = ok;
                    }
                    if !ok {
                        self.out.extend_from_slice(b"%!(BADPREC)");
                    }
                    after_index = false;
                } else {
                    let (p, ok, ni) = parse_num(format, i, end);
                    i = ni;
                    self.f.prec = p;
                    self.f.prec_present = true;
                    if !ok {
                        self.f.prec = 0;
                    }
                }
            }

            if !after_index {
                // The flag itself is dead past this point (each verb
                // re-derives it), only the position/argnum matter.
                let (an, ni, _) = self.arg_number(arg_num, format, i, args.len());
                arg_num = an;
                i = ni;
            }

            if i >= end {
                self.out.extend_from_slice(b"%!(NOVERB)");
                break;
            }

            // Decode the verb rune (may be multi-byte).
            let (verb, size) = decode_rune(format, i);
            i += size;

            if verb == '%' {
                self.out.push(b'%');
            } else if !self.good_arg_num {
                self.bad_arg_num(verb);
            } else if arg_num >= args.len() {
                self.missing_arg(verb);
            } else if verb == 'w' {
                self.f.sharp_v = self.f.sharp;
                self.f.sharp = false;
                self.f.plus_v = self.f.plus;
                self.f.plus = false;
                self.print_arg_w(&args[arg_num]);
                arg_num += 1;
            } else {
                if verb == 'v' {
                    self.f.sharp_v = self.f.sharp;
                    self.f.sharp = false;
                    self.f.plus_v = self.f.plus;
                    self.f.plus = false;
                }
                self.print_arg(&args[arg_num], verb);
                arg_num += 1;
            }
        }

        if !self.reordered && arg_num < args.len() {
            self.f = Flags::default();
            self.out.extend_from_slice(b"%!(EXTRA ");
            for (i, arg) in args[arg_num..].iter().enumerate() {
                if i > 0 {
                    self.out.extend_from_slice(b", ");
                }
                if matches!(arg, Value::Nil) {
                    self.out.extend_from_slice(b"<nil>");
                } else {
                    self.out.extend_from_slice(arg.type_name().as_bytes());
                    self.out.push(b'=');
                    self.print_arg(arg, 'v');
                }
            }
            self.out.push(b')');
        }
    }

    /// `%w` of a non-error: `handleMethods` badVerbs it (print.go:625).
    fn print_arg_w(&mut self, arg: &Value<'_>) {
        if matches!(arg, Value::Nil) {
            // printArg's nil check comes first: %w of nil → badVerb.
            self.bad_verb('w', None);
            return;
        }
        match arg {
            // Simple types never reach handleMethods; their dispatchers
            // badVerb on 'w'.
            Value::Bool(b) => self.dispatch_bool(*b, 'w', arg),
            Value::Float(f) => self.dispatch_float(*f, 'w', arg),
            Value::Complex(re, im) => self.dispatch_complex(*re, *im, 'w', arg),
            Value::Int(n, _) => self.dispatch_integer(*n as u64, true, 'w', arg),
            Value::Uint(n, _) => self.dispatch_integer(*n, false, 'w', arg),
            Value::Str(s) => self.dispatch_string(s, 'w', arg),
            Value::Bytes(b) => self.dispatch_bytes(b, 'w', "[]byte", arg),
            _ => {
                if !self.handle_methods(arg, 'w') {
                    self.print_value(arg, 'w', 0);
                }
            }
        }
    }

    /// `pp.argNumber` + `parseArgNumber`: `%[n]` handling.
    fn arg_number(
        &mut self,
        arg_num: usize,
        format: &[u8],
        i: usize,
        num_args: usize,
    ) -> (usize, usize, bool) {
        if i >= format.len() || format[i] != b'[' {
            return (arg_num, i, false);
        }
        self.reordered = true;
        // parseArgNumber(format[i:]) → (index, wid, ok).
        let (index, wid, ok) = {
            let f = &format[i..];
            if f.len() < 3 {
                (0usize, 1usize, false)
            } else {
                let mut result = (0usize, 1usize, false);
                for j in 1..f.len() {
                    if f[j] == b']' {
                        let (width, ok, newi) = parse_num(f, 1, j);
                        if !ok || newi != j || width == 0 {
                            result = (0, j + 1, false);
                        } else {
                            // one-indexed
                            result = (width - 1, j + 1, true);
                        }
                        break;
                    }
                    result = (0, 1, false);
                }
                result
            }
        };
        if ok && index < num_args {
            return (index, i + wid, true);
        }
        self.good_arg_num = false;
        (arg_num, i + wid, ok)
    }
}

// ---------------------------------------------------------------------
// Module-scope helpers
// ---------------------------------------------------------------------

fn push_char(out: &mut Vec<u8>, c: char) {
    let mut buf = [0u8; 4];
    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
}

/// `fmt.truncateString`: the byte length of the first `prec` runes.
fn truncate_len(s: &[u8], prec: Option<usize>) -> usize {
    let Some(mut n) = prec else { return s.len() };
    let mut i = 0;
    while i < s.len() {
        if n == 0 {
            return i;
        }
        n -= 1;
        i += rune_len(s, i);
    }
    s.len()
}

/// UTF-8 rune count over possibly-invalid bytes (each invalid byte is
/// one rune, like Go's `utf8.RuneCount`).
fn rune_count(b: &[u8]) -> usize {
    let mut n = 0;
    let mut i = 0;
    while i < b.len() {
        i += rune_len(b, i);
        n += 1;
    }
    n
}

/// The byte length of the rune starting at `b[i]` (1 for an invalid
/// byte, like Go's DecodeRune).
fn rune_len(b: &[u8], i: usize) -> usize {
    let first = b[i];
    let len = match first {
        0x00..=0x7F => 1,
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return 1,
    };
    if i + len > b.len() {
        return 1;
    }
    match std::str::from_utf8(&b[i..i + len]) {
        Ok(_) => len,
        Err(_) => 1,
    }
}

/// Decodes the rune at `b[i]`, returning (rune, size); invalid bytes
/// yield (U+FFFD, 1) like Go's `utf8.DecodeRune`.
fn decode_rune(b: &[u8], i: usize) -> (char, usize) {
    let len = rune_len(b, i);
    match std::str::from_utf8(&b[i..i + len]) {
        Ok(s) => (s.chars().next().unwrap_or('\u{FFFD}'), len),
        Err(_) => ('\u{FFFD}', 1),
    }
}

/// `parsenum` from print.go: decimal scan with the width cap.
fn parse_num(s: &[u8], start: usize, end: usize) -> (usize, bool, usize) {
    if start >= end {
        return (0, false, end);
    }
    let mut num = 0usize;
    let mut is_num = false;
    let mut new_i = start;
    while new_i < end && s[new_i].is_ascii_digit() {
        const TOO_LARGE: usize = 1 << 30;
        if num >= TOO_LARGE {
            return (0, false, end); // Overflow; crazy long number.
        }
        num = num * 10 + (s[new_i] - b'0') as usize;
        is_num = true;
        new_i += 1;
    }
    (num, is_num, new_i)
}

/// `intFromArg`: reads a `*` width/precision operand (must be an
/// integer kind; bools/etc fail).
fn int_from_arg(args: &[Value<'_>], arg_num: usize) -> (i64, bool, usize) {
    if arg_num >= args.len() {
        return (0, false, arg_num);
    }
    let v = &args[arg_num];
    let out = match v {
        Value::Int(n, _) => Some(*n),
        Value::Uint(n, _) => i64::try_from(*n).ok(),
        Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => Some(*n),
        _ => None,
    };
    match out {
        Some(n) => {
            // Go caps at 1e6 via tooLarge.
            if n.unsigned_abs() > 1_000_000 {
                (0, false, arg_num + 1)
            } else {
                (n, true, arg_num + 1)
            }
        }
        None => (0, false, arg_num + 1),
    }
}

// ---------------------------------------------------------------------
// Quoting (strconv ports, byte-string aware)
// ---------------------------------------------------------------------

/// Go `strconv.IsPrint` — re-exported from the pulsus-promql port
/// (verbatim go table transcription, full-codepoint-space checksummed).
pub fn is_print(c: char) -> bool {
    pulsus_promql::eval::quote::go_is_print(c)
}

/// `strconv.Quote` over raw bytes: invalid UTF-8 bytes become `\xNN`.
pub fn quote_bytes(s: &[u8], quote: char) -> String {
    quote_with(s, quote, false)
}

/// `strconv.QuoteToASCII` over raw bytes.
pub fn quote_bytes_ascii(s: &[u8]) -> String {
    quote_with(s, '"', true)
}

fn quote_with(s: &[u8], quote: char, ascii_only: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    let mut i = 0;
    while i < s.len() {
        let width = rune_len(s, i);
        if width == 1 && s[i] >= 0x80 {
            // Invalid byte.
            let _ = write!(out, "\\x{:02x}", s[i]);
            i += 1;
            continue;
        }
        let r = std::str::from_utf8(&s[i..i + width])
            .ok()
            .and_then(|t| t.chars().next())
            .unwrap_or('\u{FFFD}');
        i += width;
        append_escaped_rune(&mut out, r, quote, ascii_only);
    }
    out.push(quote);
    out
}

/// `strconv.appendEscapedRune`.
fn append_escaped_rune(out: &mut String, r: char, quote: char, ascii_only: bool) {
    use std::fmt::Write as _;
    if r == quote || r == '\\' {
        out.push('\\');
        out.push(r);
        return;
    }
    if ascii_only {
        if (r as u32) < 0x80 && is_print(r) {
            out.push(r);
            return;
        }
    } else if is_print(r) {
        out.push(r);
        return;
    }
    match r {
        '\x07' => out.push_str("\\a"),
        '\x08' => out.push_str("\\b"),
        '\x0c' => out.push_str("\\f"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '\x0b' => out.push_str("\\v"),
        c if (c as u32) < 0x20 || c == '\x7f' => {
            let _ = write!(out, "\\x{:02x}", c as u32);
        }
        c if (c as u32) < 0x10000 => {
            let _ = write!(out, "\\u{:04x}", c as u32);
        }
        c => {
            let _ = write!(out, "\\U{:08x}", c as u32);
        }
    }
}

/// `strconv.QuoteRune` / `QuoteRuneToASCII`.
pub fn quote_rune(r: char, ascii_only: bool) -> String {
    let mut out = String::with_capacity(8);
    out.push('\'');
    append_escaped_rune(&mut out, r, '\'', ascii_only);
    out.push('\'');
    out
}

/// `strconv.CanBackquote` over bytes.
fn can_backquote(s: &[u8]) -> bool {
    let mut i = 0;
    while i < s.len() {
        let width = rune_len(s, i);
        if width == 1 && s[i] >= 0x80 {
            return false; // invalid byte (RuneError of size 1)
        }
        let r = match std::str::from_utf8(&s[i..i + width])
            .ok()
            .and_then(|t| t.chars().next())
        {
            Some(r) => r,
            None => return false,
        };
        i += width;
        if width > 1 {
            if r == '\u{FEFF}' {
                return false;
            }
            continue;
        }
        if (r as u32) < 0x20 && r != '\t' {
            return false;
        }
        if r == '`' || r == '\u{7F}' {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------
// Float formatting (strconv.FormatFloat port for e E f g G b x X)
// ---------------------------------------------------------------------

/// `strconv.FormatFloat(v, verb, prec, 64)`. `prec < 0` = shortest.
pub fn format_float_go(v: f64, verb: char, prec: i32) -> Vec<u8> {
    if v.is_nan() {
        return b"NaN".to_vec();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            b"+Inf".to_vec()
        } else {
            b"-Inf".to_vec()
        };
    }
    match verb {
        'b' => format_float_b(v),
        'x' => format_float_hex(v, prec, false),
        'X' => format_float_hex(v, prec, true),
        'e' | 'E' => {
            let (neg, digits, exp) = decimal_digits(
                v,
                if prec < 0 {
                    None
                } else {
                    Some(prec as usize + 1)
                },
            );
            fmt_e(neg, &digits, exp, prec, verb == 'E')
        }
        'f' | 'F' => {
            let p = if prec < 0 {
                // Shortest 'f': all shortest digits in fixed form.
                let (neg, digits, exp) = decimal_digits(v, None);
                return fmt_f_from_digits(neg, &digits, exp);
            } else {
                prec as usize
            };
            format!("{v:.p$}").into_bytes()
        }
        'g' | 'G' => {
            let upper = verb == 'G';
            let shortest = prec < 0;
            if shortest {
                let (neg, digits, exp10) = decimal_digits(v, None);
                let exp = exp10 - 1; // decimal exponent of leading digit
                let eprec = 6;
                if exp < -4 || exp >= eprec {
                    fmt_e(neg, &digits, exp10, digits.len() as i32 - 1, upper)
                } else {
                    fmt_f_from_digits(neg, &digits, exp10)
                }
            } else {
                let mut p = prec;
                if p == 0 {
                    p = 1;
                }
                let (neg, mut digits, exp10) = decimal_digits(v, Some(p as usize));
                // eprec per ftoa.go.
                let mut eprec = p;
                if eprec > digits.len() as i32 && digits.len() as i32 >= exp10 {
                    eprec = digits.len() as i32;
                }
                let exp = exp10 - 1;
                if exp < -4 || exp >= eprec {
                    // Trailing zeros removed.
                    while digits.len() > 1 && digits.last() == Some(&b'0') {
                        digits.pop();
                    }
                    let e_prec = (digits.len() as i32 - 1).min(p - 1);
                    fmt_e(neg, &digits, exp10, e_prec, upper)
                } else {
                    // %f form with prec = max(p - exp10, 0), trailing
                    // zeros removed.
                    while digits.len() > 1 && digits.last() == Some(&b'0') {
                        digits.pop();
                    }
                    fmt_f_from_digits(neg, &digits, exp10)
                }
            }
        }
        _ => format!("%!{verb}(float64={v})").into_bytes(),
    }
}

/// Correctly-rounded decimal digits of `v`: returns (neg, digits,
/// exp10) with `value = 0.digits × 10^exp10`. `sig` = significant digit
/// count (None = shortest round-trip). Uses Rust's exact `{:e}`/
/// `{:.p$e}` formatting (arbitrary-precision Dragon), so rounding
/// matches strconv.
fn decimal_digits(v: f64, sig: Option<usize>) -> (bool, Vec<u8>, i32) {
    let s = match sig {
        None => format!("{v:e}"),
        Some(n) => format!("{v:.p$e}", p = n.saturating_sub(1)),
    };
    let (mant, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let neg = mant.starts_with('-');
    let mant = mant.strip_prefix('-').unwrap_or(mant);
    let digits: Vec<u8> = mant.bytes().filter(|b| b.is_ascii_digit()).collect();
    // value = d.ddd × 10^exp = 0.digits × 10^(exp+1)
    (neg, digits, exp + 1)
}

/// Renders `0.digits × 10^exp10` in Go `%e` form with `prec` fraction
/// digits (prec < 0 = all).
fn fmt_e(neg: bool, digits: &[u8], exp10: i32, prec: i32, upper: bool) -> Vec<u8> {
    let mut out = Vec::new();
    if neg {
        out.push(b'-');
    }
    out.push(*digits.first().unwrap_or(&b'0'));
    let frac: &[u8] = if digits.len() > 1 { &digits[1..] } else { &[] };
    let want = if prec < 0 { frac.len() } else { prec as usize };
    if want > 0 {
        out.push(b'.');
        for k in 0..want {
            out.push(*frac.get(k).unwrap_or(&b'0'));
        }
    }
    out.push(if upper { b'E' } else { b'e' });
    let exp = if digits == b"0" { 0 } else { exp10 - 1 };
    if exp >= 0 {
        out.push(b'+');
    } else {
        out.push(b'-');
    }
    let e = exp.unsigned_abs();
    if e < 10 {
        out.push(b'0');
    }
    out.extend_from_slice(e.to_string().as_bytes());
    out
}

/// Renders `0.digits × 10^exp10` in fixed form with no trailing zeros
/// beyond the digits given.
fn fmt_f_from_digits(neg: bool, digits: &[u8], exp10: i32) -> Vec<u8> {
    let mut out = Vec::new();
    if neg {
        out.push(b'-');
    }
    if digits == b"0" {
        out.push(b'0');
        return out;
    }
    let point = exp10; // digits before the decimal point
    if point <= 0 {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', (-point) as usize));
        out.extend_from_slice(digits);
    } else if (point as usize) >= digits.len() {
        out.extend_from_slice(digits);
        out.extend(std::iter::repeat_n(b'0', point as usize - digits.len()));
    } else {
        out.extend_from_slice(&digits[..point as usize]);
        out.push(b'.');
        out.extend_from_slice(&digits[point as usize..]);
    }
    out
}

/// `%b`: decimal mantissa × 2^exp (`6755399441055744p-50` style).
fn format_float_b(v: f64) -> Vec<u8> {
    let bits = v.to_bits();
    let neg = bits >> 63 == 1;
    let biased = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & 0xF_FFFF_FFFF_FFFF;
    let (mant, exp) = if biased == 0 {
        (frac, -1074)
    } else {
        (frac | (1 << 52), biased - 1075)
    };
    let mut out = Vec::new();
    if neg {
        out.push(b'-');
    }
    out.extend_from_slice(mant.to_string().as_bytes());
    out.push(b'p');
    if exp >= 0 {
        out.push(b'+');
    }
    out.extend_from_slice(exp.to_string().as_bytes());
    out
}

/// `%x`/`%X` hex float (strconv `fmtX`): normalized `0x1.<frac>p±dd`,
/// shortest (trailing zero nibbles trimmed) or rounded to `prec`
/// nibbles (round half even on the cut bit run).
fn format_float_hex(v: f64, prec: i32, upper: bool) -> Vec<u8> {
    let bits = v.to_bits();
    let neg = bits >> 63 == 1;
    let biased = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & 0xF_FFFF_FFFF_FFFF;
    let (mut mant, mut exp) = if biased == 0 {
        if frac == 0 {
            // Zero.
            let mut out: Vec<u8> = Vec::new();
            if neg {
                out.push(b'-');
            }
            out.extend_from_slice(if upper { b"0X0" } else { b"0x0" });
            if prec > 0 {
                out.push(b'.');
                out.extend(std::iter::repeat_n(b'0', prec as usize));
            }
            out.extend_from_slice(if upper { b"P+00" } else { b"p+00" });
            return out;
        }
        // Denormal: normalize.
        let shift = frac.leading_zeros() as i32 - 11;
        ((frac << shift), -1022 - shift)
    } else {
        (frac | (1 << 52), biased - 1023)
    };
    // mant currently has the leading 1 at bit 52; value = mant × 2^(exp-52).
    // Round to prec nibbles of fraction if requested (52 bits = 13 nibbles).
    if prec >= 0 {
        let keep_bits = (4 * prec).min(52);
        let drop = 52 - keep_bits;
        if drop > 0 {
            let round_bit = 1u64 << (drop - 1);
            let mask = (1u64 << drop) - 1;
            let rem = mant & mask;
            mant >>= drop;
            if rem > round_bit || (rem == round_bit && mant & 1 == 1) {
                mant += 1;
                // Carry past the leading 1: renormalize.
                if mant >> (keep_bits + 1) != 0 {
                    mant >>= 1;
                    exp += 1;
                }
            }
            mant <<= drop;
        }
    }
    // Extract nibbles: fraction = bits 51..0, grouped high-first into 13
    // nibbles (pad low end to nibble boundary).
    let frac_bits = mant & ((1u64 << 52) - 1);
    let padded = frac_bits; // 52 bits; nibble-align from the top
    let mut nibbles: Vec<u8> = (0..13)
        .map(|k| ((padded >> (48 - 4 * k)) & 0xF) as u8)
        .collect();
    match prec {
        p if p < 0 => {
            while nibbles.last() == Some(&0) {
                nibbles.pop();
            }
        }
        p => {
            nibbles.truncate(p as usize);
            while (nibbles.len() as i32) < p {
                nibbles.push(0);
            }
        }
    }
    let digits: &[u8] = if upper {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    let mut out = Vec::new();
    if neg {
        out.push(b'-');
    }
    out.extend_from_slice(if upper { b"0X1" } else { b"0x1" });
    if !nibbles.is_empty() {
        out.push(b'.');
        for n in &nibbles {
            out.push(digits[*n as usize]);
        }
    }
    out.push(if upper { b'P' } else { b'p' });
    if exp >= 0 {
        out.push(b'+');
    } else {
        out.push(b'-');
    }
    let e = exp.unsigned_abs();
    if e < 10 {
        out.push(b'0');
    }
    out.extend_from_slice(e.to_string().as_bytes());
    out
}
