//! Trusted tenant context shared by optional Rustee integrations.
//!
//! A [`TenantContext`] is created only from verified identity or trusted server-side routing. It
//! is intentionally distinct from arbitrary client-provided request data so persistence adapters
//! can require the same scope that authorization middleware resolved.

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{Error, FromRequest, Request, RouteParams, StateStore};

/// A tenant selected by trusted server-side routing, identity, or session lookup.
///
/// This type must never be constructed directly from a client-controlled header. Authentication
/// middleware verifies the resolved context against the authenticated principal before it becomes
/// available to a handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantContext(String);

impl TenantContext {
    /// Creates one non-blank tenant context from a trusted server-side source.
    ///
    /// # Errors
    ///
    /// Returns [`TenantContextError::BlankTenant`] when `tenant` is blank.
    pub fn new(tenant: impl Into<String>) -> Result<Self, TenantContextError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() {
            return Err(TenantContextError::BlankTenant);
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
}

#[cfg(test)]
mod tests {
    use super::{TenantContext, TenantContextError};

    #[test]
    fn tenant_context_rejects_blank_values() {
        assert_eq!(
            TenantContext::new(" ").unwrap_err(),
            TenantContextError::BlankTenant
        );
    }
}
