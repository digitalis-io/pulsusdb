//! The Connect protocol's error rule, shared by every profiles route.

/// A Connect error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    InvalidArgument,
    ResourceExhausted,
    Unimplemented,
    Unavailable,
    Internal,
    Unknown,
}

impl Code {
    /// Stub.
    pub fn as_str(self) -> &'static str {
        ""
    }

    /// Stub.
    pub fn http_status(self) -> u16 {
        0
    }

    /// Stub.
    pub fn from_http_status(_status: u16) -> Code {
        Code::Unknown
    }
}

/// Stub.
pub fn error_json(_code: Code, _message: &str) -> String {
    String::new()
}
