//! Trusted tenant context shared by optional Rustee integrations.
//!
//! A [`TenantContext`] is created only from verified identity or trusted server-side routing. It
//! is intentionally distinct from arbitrary client-provided request data so persistence adapters
//! can require the same scope that authorization middleware resolved.

use std::fmt;

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{Error, FromRequest, Request, RouteParams, StateStore};

/// Maximum UTF-8 byte length of one trusted tenant identifier.
pub const MAX_TENANT_IDENTIFIER_BYTES: usize = 1024;

/// A tenant selected by trusted server-side routing, identity, or session lookup.
///
/// This type must never be constructed directly from a client-controlled header. Authentication
/// middleware verifies the resolved context against the authenticated principal before it becomes
/// available to a handler.
#[derive(Clone, Eq, PartialEq)]
pub struct TenantContext(String);

impl fmt::Debug for TenantContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TenantContext([REDACTED])")
    }
}

impl TenantContext {
    /// Creates one non-blank, NUL-free tenant context from a trusted server-side source.
    ///
    /// # Errors
    ///
    /// Returns [`TenantContextError::BlankTenant`] when `tenant` is blank,
    /// [`TenantContextError::ValueTooLong`] when it exceeds [`MAX_TENANT_IDENTIFIER_BYTES`], or
    /// [`TenantContextError::TenantContainsNul`] when it cannot be represented in every supported
    /// persistence boundary.
    pub fn new(tenant: impl Into<String>) -> Result<Self, TenantContextError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() {
            return Err(TenantContextError::BlankTenant);
        }
        if tenant.len() > MAX_TENANT_IDENTIFIER_BYTES {
            return Err(TenantContextError::ValueTooLong);
        }
        if tenant.contains('\0') {
            return Err(TenantContextError::TenantContainsNul);
        }
        Ok(Self(tenant))
    }

    /// Returns the server-selected tenant identifier.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.0
    }
}

impl FromRequest for TenantContext {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "tenant_context_missing",
                    "trusted tenant context is required",
                )
            })
        })
    }
}

/// Invalid trusted tenant context configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TenantContextError {
    /// The server-selected tenant was blank.
    #[error("a tenant context must not be blank")]
    BlankTenant,
    /// The server-selected tenant exceeded its bounded identifier size.
    #[error("a tenant context exceeds the 1024-byte limit")]
    ValueTooLong,
    /// The server-selected tenant could not be represented in every supported persistence adapter.
    #[error("a tenant context must not contain a NUL byte")]
    TenantContainsNul,
}

#[cfg(test)]
mod tests {
    use super::{MAX_TENANT_IDENTIFIER_BYTES, TenantContext, TenantContextError};

    #[test]
    fn tenant_context_rejects_blank_values() {
        assert_eq!(
            TenantContext::new(" ").unwrap_err(),
            TenantContextError::BlankTenant
        );
    }

    #[test]
    fn tenant_context_rejects_oversized_values() {
        assert_eq!(
            TenantContext::new("t".repeat(MAX_TENANT_IDENTIFIER_BYTES + 1)).unwrap_err(),
            TenantContextError::ValueTooLong
        );
    }

    #[test]
    fn tenant_context_rejects_nul_before_persistence_adapters_receive_it() {
        assert_eq!(
            TenantContext::new("tenant\0invalid").unwrap_err(),
            TenantContextError::TenantContainsNul
        );
    }

    #[test]
    fn tenant_context_debug_redacts_the_tenant_identifier() {
        let tenant = TenantContext::new("private-tenant").unwrap();

        assert_eq!(format!("{tenant:?}"), "TenantContext([REDACTED])");
    }
}
