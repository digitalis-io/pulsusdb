//! The tenant a metrics request reads or writes (issue #635): the validated
//! value of its `X-Scope-OrgID` header, stored as `org_id`, the first column
//! of every metrics table's sorting key.
//!
//! [`Tenant::from_header`] is the only constructor, so every `Tenant` holds
//! a value that passed its rule.

use std::sync::Arc;

/// The header a request names its tenant in.
pub const TENANT_HEADER: &str = "x-scope-orgid";

/// The longest tenant, in bytes.
pub const MAX_TENANT_BYTES: usize = 150;

/// A validated tenant: the empty tenant `''` when the request named none,
/// else 1 to [`MAX_TENANT_BYTES`] bytes, each in `[A-Za-z0-9_.-]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tenant(Arc<str>);

/// The one refusal: the header's value is not a tenant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid X-Scope-OrgID")]
pub struct TenantError;

impl Tenant {
    /// The tenant of a request whose `X-Scope-OrgID` is `value`, given
    /// `repeated` when the header appears more than once. No header is the
    /// empty tenant; anything else outside the rule is [`TenantError`].
    pub fn from_header(
        value: Option<&http::HeaderValue>,
        repeated: bool,
    ) -> Result<Tenant, TenantError> {
        if repeated {
            return Err(TenantError);
        }
        let Some(value) = value else {
            return Ok(Tenant(Arc::from("")));
        };
        let bytes = value.as_bytes();
        let valid = !bytes.is_empty()
            && bytes.len() <= MAX_TENANT_BYTES
            && bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
        if !valid {
            return Err(TenantError);
        }
        // Every byte is ASCII, so the text is the bytes.
        let text = std::str::from_utf8(bytes).map_err(|_| TenantError)?;
        Ok(Tenant(Arc::from(text)))
    }

    /// The tenant's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The tenant's text as the shared string a landing row carries.
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }
}

/// [`Tenant::from_header`] over a request's headers: the first
/// `X-Scope-OrgID`, and whether there is more than one.
pub fn tenant_from_headers(headers: &http::HeaderMap) -> Result<Tenant, TenantError> {
    let mut all = headers.get_all(TENANT_HEADER).iter();
    let first = all.next();
    let repeated = all.next().is_some();
    Tenant::from_header(first, repeated)
}
