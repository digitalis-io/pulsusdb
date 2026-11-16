//! The Connect protocol's error rule, shared by every profiles route: a
//! code, its HTTP status (`protocol_connect.go:1269-1308` of the Connect
//! library), and the JSON error body.

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
    for c in message.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push_str("\"}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_error_body_escapes_as_go_does() {
        assert_eq!(
            error_json(Code::InvalidArgument, "a \"b\" <c> & \\ \n\u{1}"),
            r#"{"code":"invalid_argument","message":"a \"b\" \u003cc\u003e \u0026 \\ \n\u0001"}"#
        );
    }
}
