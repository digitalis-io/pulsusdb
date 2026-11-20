//! The Connect protocol's error rule, shared by every profiles route: a
//! code, its HTTP status (`protocol_connect.go:1269-1308` of the Connect
//! library), and the JSON error body. Issue #829: the string escaping it
//! uses ([`go_string`]), shared with the render routes' answers.

/// A Connect error code: the ones the profiles routes answer, ingest and
/// query alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    InvalidArgument,
    ResourceExhausted,
    Unimplemented,
    Unavailable,
    Internal,
    Unknown,
    DeadlineExceeded,
}

impl Code {
    /// The code's wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Code::InvalidArgument => "invalid_argument",
            Code::ResourceExhausted => "resource_exhausted",
            Code::Unimplemented => "unimplemented",
            Code::Unavailable => "unavailable",
            Code::Internal => "internal",
            Code::Unknown => "unknown",
            Code::DeadlineExceeded => "deadline_exceeded",
        }
    }

    /// The HTTP status a Connect unary error with this code is sent with.
    pub fn http_status(self) -> u16 {
        match self {
            Code::InvalidArgument => 400,
            Code::ResourceExhausted => 429,
            Code::Unimplemented => 501,
            Code::Unavailable => 503,
            Code::Internal | Code::Unknown => 500,
            Code::DeadlineExceeded => 504,
        }
    }

    /// The code the reference's plain-HTTP error writer names for `status`
    /// (`pkg/util/connectgrpc/connectgrpc.go:273-302`), for the codes above.
    pub fn from_http_status(status: u16) -> Code {
        match status {
            400 | 413 => Code::InvalidArgument,
            404 => Code::Unimplemented,
            429 | 431 => Code::ResourceExhausted,
            502 | 503 => Code::Unavailable,
            504 => Code::DeadlineExceeded,
            _ => Code::Unknown,
        }
    }
}

/// `{"code":"<code>","message":"<message>"}`, with no newline, the message
/// escaped as Go's `json.Marshal` escapes it.
pub fn error_json(code: Code, message: &str) -> String {
    let mut out = String::with_capacity(message.len() + 40);
    out.push_str(r#"{"code":""#);
    out.push_str(code.as_str());
    out.push_str(r#"","message":""#);
    go_string(&mut out, message);
    out.push_str("\"}");
    out
}

/// Issue #829: `s`'s characters as Go's `json.Marshal` escapes a string's,
/// without the quotes: `"` and `\\`, the five short control escapes, any
/// other control character as `\u00xx`, and `<`, `>`, `&`, U+2028 and
/// U+2029 as `\u` escapes.
pub fn go_string(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
}

/// Issue #829: `v` as Go's `json.Encoder` writes a `[]string`: `null` when
/// empty, else the strings in order, each by [`go_string`]; then `\n`.
pub fn go_string_array(v: &[String]) -> String {
    if v.is_empty() {
        return "null\n".to_string();
    }
    let mut out = String::with_capacity(v.iter().map(|s| s.len() + 3).sum::<usize>() + 3);
    out.push('[');
    for (i, s) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        go_string(&mut out, s);
        out.push('"');
    }
    out.push_str("]\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_error_body_escapes_as_go_does() {
        assert_eq!(
            error_json(Code::InvalidArgument, "a \"b\" <c> & \\ \n\u{1}\u{8}\u{c}"),
            r#"{"code":"invalid_argument","message":"a \"b\" \u003cc\u003e \u0026 \\ \n\u0001\b\f"}"#
        );
    }

    /// Issue #829: a `[]string` as Go's `json.Encoder` writes it.
    #[test]
    fn go_string_array_writes_null_or_the_escaped_strings() {
        assert_eq!(go_string_array(&[]), "null\n");
        assert_eq!(
            go_string_array(&["a<b".to_string(), "c".to_string()]),
            "[\"a\\u003cb\",\"c\"]\n"
        );
    }
}
