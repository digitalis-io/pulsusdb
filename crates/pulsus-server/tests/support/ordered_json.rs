//! An ORDER-PRESERVING JSON value, for comparisons where key order is
//! part of the contract (issue #463).
//!
//! `serde_json::Value` backs its objects with a `BTreeMap`, so parsing
//! and re-serialising sorts the keys. Two of this issue's wire rules are
//! about order and would be invisible through that: `encodingFlags`
//! must precede `result` in the query envelope and follow `streams` in
//! the tail frame, and `structuredMetadata` must precede `parsed` inside
//! an entry's third element. Enabling `serde_json`'s `preserve_order`
//! feature would change every map this workspace serialises, which is
//! not a change this issue makes — so the comparison carries its own
//! reader instead.
//!
//! Deliberately small: parse, index, elide a key by name, re-serialise.
//! No `Deserialize` derive, no borrowing, no numbers beyond what a
//! response body contains.

#![allow(dead_code)]

use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// Kept as its SOURCE TEXT: a response's timestamps are exact
    /// decimal strings and re-rendering them through `f64` would move
    /// bytes the comparison is about.
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    /// Insertion order preserved.
    Obj(Vec<(String, Json)>),
}

impl Json {
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn arr(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(v) => Some(v),
            _ => None,
        }
    }

    pub fn obj(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Obj(v) => Some(v),
            _ => None,
        }
    }

    pub fn str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Removes every object member named `key`, at any depth.
    pub fn elide(&mut self, key: &str) {
        match self {
            Json::Obj(pairs) => {
                pairs.retain(|(k, _)| k != key);
                for (_, v) in pairs.iter_mut() {
                    v.elide(key);
                }
            }
            Json::Arr(items) => {
                for v in items {
                    v.elide(key);
                }
            }
            _ => {}
        }
    }

    /// Rewrites every string equal to, or containing, `from` so that
    /// `from` becomes `to` — the per-run nonce absorber.
    pub fn substitute(&mut self, from: &str, to: &str) {
        match self {
            Json::Str(s) => {
                if s.contains(from) {
                    *s = s.replace(from, to);
                }
            }
            Json::Obj(pairs) => {
                for (k, v) in pairs.iter_mut() {
                    if k.contains(from) {
                        *k = k.replace(from, to);
                    }
                    v.substitute(from, to);
                }
            }
            Json::Arr(items) => {
                for v in items {
                    v.substitute(from, to);
                }
            }
            _ => {}
        }
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => {
                let _ = write!(out, "{b}");
            }
            Json::Num(n) => out.push_str(n),
            Json::Str(s) => {
                out.push_str(&serde_json::to_string(s).expect("string encodes"));
            }
            Json::Arr(items) => {
                out.push('[');
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.write(out);
                }
                out.push(']');
            }
            Json::Obj(pairs) => {
                out.push('{');
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(k).expect("key encodes"));
                    out.push(':');
                    v.write(out);
                }
                out.push('}');
            }
        }
    }
}

pub fn parse(text: &str) -> Result<Json, String> {
    let b = text.as_bytes();
    let mut i = 0usize;
    let v = value(b, &mut i)?;
    skip_ws(b, &mut i);
    if i != b.len() {
        return Err(format!("trailing bytes at {i}"));
    }
    Ok(v)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && matches!(b[*i], b' ' | b'\t' | b'\n' | b'\r') {
        *i += 1;
    }
}

fn value(b: &[u8], i: &mut usize) -> Result<Json, String> {
    skip_ws(b, i);
    match b.get(*i) {
        None => Err("unexpected end".to_string()),
        Some(b'{') => {
            *i += 1;
            let mut pairs = Vec::new();
            skip_ws(b, i);
            if b.get(*i) == Some(&b'}') {
                *i += 1;
                return Ok(Json::Obj(pairs));
            }
            loop {
                skip_ws(b, i);
                let Json::Str(k) = value(b, i)? else {
                    return Err(format!("object key at {i} is not a string"));
                };
                skip_ws(b, i);
                if b.get(*i) != Some(&b':') {
                    return Err(format!("expected ':' at {i}"));
                }
                *i += 1;
                let v = value(b, i)?;
                pairs.push((k, v));
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b'}') => {
                        *i += 1;
                        return Ok(Json::Obj(pairs));
                    }
                    _ => return Err(format!("expected ',' or '}}' at {i}")),
                }
            }
        }
        Some(b'[') => {
            *i += 1;
            let mut items = Vec::new();
            skip_ws(b, i);
            if b.get(*i) == Some(&b']') {
                *i += 1;
                return Ok(Json::Arr(items));
            }
            loop {
                items.push(value(b, i)?);
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b']') => {
                        *i += 1;
                        return Ok(Json::Arr(items));
                    }
                    _ => return Err(format!("expected ',' or ']' at {i}")),
                }
            }
        }
        Some(b'"') => {
            let start = *i;
            *i += 1;
            while *i < b.len() {
                match b[*i] {
                    b'\\' => *i += 2,
                    b'"' => {
                        *i += 1;
                        let raw = text_of(b, start, *i);
                        let s: String = serde_json::from_str(&raw)
                            .map_err(|e| format!("string at {start}: {e}"))?;
                        return Ok(Json::Str(s));
                    }
                    _ => *i += 1,
                }
            }
            Err(format!("unterminated string at {start}"))
        }
        Some(b't') if b[*i..].starts_with(b"true") => {
            *i += 4;
            Ok(Json::Bool(true))
        }
        Some(b'f') if b[*i..].starts_with(b"false") => {
            *i += 5;
            Ok(Json::Bool(false))
        }
        Some(b'n') if b[*i..].starts_with(b"null") => {
            *i += 4;
            Ok(Json::Null)
        }
        Some(_) => {
            let start = *i;
            while *i < b.len() && matches!(b[*i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                *i += 1;
            }
            if *i == start {
                return Err(format!("unexpected byte at {start}"));
            }
            Ok(Json::Num(text_of(b, start, *i)))
        }
    }
}

fn text_of(b: &[u8], from: usize, to: usize) -> String {
    String::from_utf8(b[from..to].to_vec()).expect("slice is valid UTF-8")
}
