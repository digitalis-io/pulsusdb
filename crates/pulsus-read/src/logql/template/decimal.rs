//! `shopspring/decimal` port — exactly the subset sprig's `addf`/
//! `subf`/`mulf`/`divf` reach (`sprig/numeric.go execDecimalOp`, vendored
//! decimal v1.4.0): `NewFromFloat` (shortest-round-trip digits),
//! `Add`/`Sub`/`Mul` (exact), `Div` = `DivRound(d2, DivisionPrecision =
//! 16)` (round half away from zero on the doubled remainder), and
//! `Float64` (`Rat().Float64()` — correctly-rounded nearest). Backed by
//! a small decimal-digit bignum (schoolbook; operand sizes are bounded
//! by f64's ~17 significant digits ± the exponent alignment, and this
//! runs only inside `*f` template functions — never on a non-template
//! path).
//!
//! The reference's failure surfaces are preserved verbatim:
//! `decimal division by 0` (panic → template `error calling divf: …`)
//! and `Cannot create a Decimal from +Inf`/`NaN`.

/// An arbitrary-precision signed decimal: `value = sign × digits ×
/// 10^exp`, digits most-significant-first with no leading zeros
/// (empty = zero).
#[derive(Debug, Clone, PartialEq)]
pub struct Dec {
    neg: bool,
    digits: Vec<u8>,
    exp: i32,
}

impl Dec {
    pub fn zero() -> Dec {
        Dec {
            neg: false,
            digits: Vec::new(),
            exp: 0,
        }
    }

    fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// `decimal.NewFromFloat` — shortest-round-trip digits (Go
    /// `roundShortest`, Rust `{:e}`: both correctly-shortest).
    pub fn from_float(v: f64) -> Result<Dec, String> {
        if v.is_nan() || v.is_infinite() {
            // Go: panic(fmt.Sprintf("Cannot create a Decimal from %v", value))
            let repr = if v.is_nan() {
                "NaN".to_string()
            } else if v > 0.0 {
                "+Inf".to_string()
            } else {
                "-Inf".to_string()
            };
            return Err(format!("Cannot create a Decimal from {repr}"));
        }
        if v == 0.0 {
            return Ok(Dec::zero());
        }
        let s = format!("{v:e}");
        let (mant, e) = s.split_once('e').unwrap_or((s.as_str(), "0"));
        let e: i32 = e.parse().unwrap_or(0);
        let neg = mant.starts_with('-');
        let mant = mant.strip_prefix('-').unwrap_or(mant);
        let digits: Vec<u8> = mant
            .bytes()
            .filter(|b| b.is_ascii_digit())
            .map(|b| b - b'0')
            .collect();
        let frac_len = digits.len() as i32 - 1;
        let mut d = Dec {
            neg,
            digits,
            exp: e - frac_len,
        };
        d.trim_leading();
        Ok(d)
    }

    fn trim_leading(&mut self) {
        let lead = self.digits.iter().take_while(|&&d| d == 0).count();
        self.digits.drain(..lead);
        if self.digits.is_empty() {
            self.neg = false;
        }
    }

    /// Rescale both to the smaller exponent (`RescalePair` — exact,
    /// since it only multiplies by powers of ten).
    fn rescale_pair(a: &Dec, b: &Dec) -> (Vec<u8>, Vec<u8>, i32) {
        let exp = a.exp.min(b.exp);
        (a.digits_at(exp), b.digits_at(exp), exp)
    }

    fn digits_at(&self, exp: i32) -> Vec<u8> {
        let mut d = self.digits.clone();
        let zeros = (self.exp - exp).max(0) as usize;
        d.extend(std::iter::repeat_n(0u8, zeros));
        d
    }

    pub fn add(&self, other: &Dec) -> Dec {
        let (a, b, exp) = Dec::rescale_pair(self, other);
        let (neg, digits) = signed_add(self.neg, &a, other.neg, &b);
        let mut d = Dec { neg, digits, exp };
        d.trim_leading();
        d
    }

    pub fn sub(&self, other: &Dec) -> Dec {
        let (a, b, exp) = Dec::rescale_pair(self, other);
        let (neg, digits) = signed_add(self.neg, &a, !other.neg, &b);
        let mut d = Dec { neg, digits, exp };
        d.trim_leading();
        d
    }

    pub fn mul(&self, other: &Dec) -> Dec {
        let digits = mag_mul(&self.digits, &other.digits);
        let mut d = Dec {
            neg: self.neg != other.neg && !digits.is_empty(),
            digits,
            exp: self.exp + other.exp,
        };
        d.trim_leading();
        d
    }

    /// `Div` = `DivRound(other, 16)`.
    pub fn div(&self, other: &Dec) -> Result<Dec, String> {
        const PRECISION: i32 = 16;
        if other.is_zero() {
            return Err("decimal division by 0".to_string());
        }
        // QuoRem(d2, precision): q integer multiple of 10^-precision.
        let scale = -PRECISION;
        let e = self.exp as i64 - other.exp as i64 - scale as i64;
        let (aa, bb): (Vec<u8>, Vec<u8>) = if e < 0 {
            (
                self.digits.clone(),
                shift_left(&other.digits, (-e) as usize),
            )
        } else {
            (shift_left(&self.digits, e as usize), other.digits.clone())
        };
        let (q_mag, r_mag) = mag_divmod(&aa, &bb);
        // Rounding: compare 2·|r|·10^precision against |d2| — i.e., in
        // magnitude space at matched scales. r's decimal exponent:
        //   e < 0 → scalerest = d.exp ; else scalerest = scale + d2.exp
        // r2 = 2|r| × 10^(scalerest + precision) vs |d2| × 10^(d2.exp).
        let scalerest = if e < 0 { self.exp } else { scale + other.exp };
        let r2_exp = scalerest + PRECISION;
        let r2_mag = mag_double(&r_mag);
        // Compare r2_mag×10^r2_exp with other.digits×10^other.exp.
        let cmp = cmp_scaled(&r2_mag, r2_exp, &other.digits, other.exp);
        let neg = self.neg != other.neg;
        let mut q = Dec {
            neg: neg && !q_mag.is_empty(),
            digits: q_mag,
            exp: scale,
        };
        q.trim_leading();
        if cmp >= 0 {
            // Round away from zero: |q| += 10^-precision.
            let one = Dec {
                neg,
                digits: vec![1],
                exp: -PRECISION,
            };
            q = q.add(&one);
        }
        Ok(q)
    }

    /// `Float64` (`Rat().Float64()` — correctly rounded).
    pub fn to_f64(&self) -> f64 {
        if self.is_zero() {
            return 0.0;
        }
        let mut s = String::with_capacity(self.digits.len() + 8);
        if self.neg {
            s.push('-');
        }
        for d in &self.digits {
            s.push((b'0' + d) as char);
        }
        s.push('e');
        s.push_str(&self.exp.to_string());
        s.parse::<f64>().unwrap_or(if self.neg {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        })
    }
}

// -- magnitude helpers (digit vectors, MSD first) -----------------------

fn shift_left(a: &[u8], zeros: usize) -> Vec<u8> {
    if a.is_empty() {
        return Vec::new();
    }
    let mut out = a.to_vec();
    out.extend(std::iter::repeat_n(0u8, zeros));
    out
}

fn mag_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if a.len() != b.len() {
        return a.len().cmp(&b.len());
    }
    a.cmp(b)
}

/// Compare a×10^ea with b×10^eb (magnitudes).
fn cmp_scaled(a: &[u8], ea: i32, b: &[u8], eb: i32) -> i32 {
    if a.is_empty() && b.is_empty() {
        return 0;
    }
    let exp = ea.min(eb);
    let aa = shift_left(a, (ea - exp) as usize);
    let bb = shift_left(b, (eb - exp) as usize);
    match mag_cmp(&aa, &bb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn mag_add(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len().max(b.len()) + 1;
    let mut out = vec![0u8; n];
    let mut carry = 0u8;
    for i in 0..n {
        let da = if i < a.len() { a[a.len() - 1 - i] } else { 0 };
        let db = if i < b.len() { b[b.len() - 1 - i] } else { 0 };
        let s = da + db + carry;
        out[n - 1 - i] = s % 10;
        carry = s / 10;
    }
    let lead = out.iter().take_while(|&&d| d == 0).count();
    out.drain(..lead.min(out.len().saturating_sub(1)));
    if out == [0] {
        return Vec::new();
    }
    out
}

/// a - b for a >= b.
fn mag_sub(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len();
    let mut out = vec![0u8; n];
    let mut borrow = 0i8;
    for i in 0..n {
        let da = a[n - 1 - i] as i8;
        let db = if i < b.len() {
            b[b.len() - 1 - i] as i8
        } else {
            0
        };
        let mut s = da - db - borrow;
        if s < 0 {
            s += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out[n - 1 - i] = s as u8;
    }
    let lead = out.iter().take_while(|&&d| d == 0).count();
    out.drain(..lead);
    out
}

fn signed_add(a_neg: bool, a: &[u8], b_neg: bool, b: &[u8]) -> (bool, Vec<u8>) {
    if a_neg == b_neg {
        return (a_neg, mag_add(a, b));
    }
    match mag_cmp(a, b) {
        std::cmp::Ordering::Equal => (false, Vec::new()),
        std::cmp::Ordering::Greater => (a_neg, mag_sub(a, b)),
        std::cmp::Ordering::Less => (b_neg, mag_sub(b, a)),
    }
}

fn mag_mul(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let mut acc = vec![0u32; a.len() + b.len()];
    for (i, &da) in a.iter().rev().enumerate() {
        for (j, &db) in b.iter().rev().enumerate() {
            acc[i + j] += da as u32 * db as u32;
        }
    }
    let mut carry = 0u32;
    for slot in acc.iter_mut() {
        let v = *slot + carry;
        *slot = v % 10;
        carry = v / 10;
    }
    debug_assert_eq!(carry, 0);
    let mut out: Vec<u8> = acc.iter().rev().map(|&d| d as u8).collect();
    let lead = out.iter().take_while(|&&d| d == 0).count();
    out.drain(..lead);
    out
}

fn mag_double(a: &[u8]) -> Vec<u8> {
    mag_add(a, a)
}

/// Schoolbook long division: (quotient, remainder). `b` non-zero.
fn mag_divmod(a: &[u8], b: &[u8]) -> (Vec<u8>, Vec<u8>) {
    if mag_cmp(a, b) == std::cmp::Ordering::Less {
        return (Vec::new(), a.to_vec());
    }
    let mut quotient: Vec<u8> = Vec::with_capacity(a.len());
    let mut rem: Vec<u8> = Vec::new();
    for &digit in a {
        // rem = rem*10 + digit
        if !rem.is_empty() || digit != 0 {
            rem.push(digit);
            let lead = rem.iter().take_while(|&&d| d == 0).count();
            rem.drain(..lead);
        }
        // Find q in 0..=9 with q*b <= rem.
        let mut q = 0u8;
        while q < 9 {
            let candidate = mag_mul(b, &[q + 1]);
            if mag_cmp(&candidate, &rem) == std::cmp::Ordering::Greater {
                break;
            }
            q += 1;
        }
        if q > 0 {
            rem = mag_sub(&rem, &mag_mul(b, &[q]));
        }
        quotient.push(q);
    }
    let lead = quotient.iter().take_while(|&&d| d == 0).count();
    quotient.drain(..lead);
    (quotient, rem)
}
