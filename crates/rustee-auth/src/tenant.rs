//! Trusted tenant resolution, tenant-context injection, and tenant isolation middleware.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderMap, StatusCode, header::HOST};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::{AuthError, Principal, TenantContext, bearer::authentication_response};

/// Resolves a tenant from a server-controlled routing, host-mapping, or session source.
///
/// Resolvers receive an authenticated principal but must not treat an arbitrary client header as a
/// tenant authority. [`TenantResolutionLayer`] verifies the resolved context against that
/// principal before the inner service can observe it.
pub trait TenantResolver: Clone + Send + Sync + 'static {
    /// Provider-specific infrastructure failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolves the context for this request, or returns `None` for an unmapped tenant.
    fn resolve(
        &self,
        request: &Request,
        principal: &Principal,
    ) -> BoxFuture<'static, Result<Option<TenantContext>, Self::Error>>;
}

/// A server-configured, exact-authority tenant resolver.
///
/// The request `Host` selects only a configured routing scope. It does not grant access: the
/// resolution layer still requires the authenticated principal to have the same tenant.
#[derive(Clone, Eq, PartialEq)]
pub struct HostTenantResolver {
    tenants: BTreeMap<String, TenantContext>,
}

impl fmt::Debug for HostTenantResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostTenantResolver")
            .field("configured_host_count", &self.tenants.len())
            .finish()
    }
}

impl HostTenantResolver {
    /// Creates a resolver from one or more configured host authority to tenant mappings.
    ///
    /// Host matching is ASCII case-insensitive and exact after HTTP authority parsing. A mapping
    /// must not use userinfo, whitespace, quotes, or a duplicate authority.
    ///
    /// # Errors
    ///
    /// Returns [`HostTenantResolverError`] for an empty map or an invalid host mapping.
    pub fn new<I, H>(hosts: I) -> Result<Self, HostTenantResolverError>
    where
        I: IntoIterator<Item = (H, TenantContext)>,
        H: Into<String>,
    {
        let mut tenants = BTreeMap::new();
        for (host, tenant) in hosts {
            let host = canonical_host(&host.into())?;
            if tenants.insert(host.clone(), tenant).is_some() {
                return Err(HostTenantResolverError::DuplicateHost);
            }
        }
        if tenants.is_empty() {
            return Err(HostTenantResolverError::EmptyMapping);
        }
        Ok(Self { tenants })
    }
}

impl TenantResolver for HostTenantResolver {
    type Error = Infallible;

    fn resolve(
        &self,
        request: &Request,
        _principal: &Principal,
    ) -> BoxFuture<'static, Result<Option<TenantContext>, Self::Error>> {
        let tenant =
            request_host(request.headers()).and_then(|host| self.tenants.get(&host).cloned());
        Box::pin(async move { Ok(tenant) })
    }
}

/// Invalid server-configured host-to-tenant mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HostTenantResolverError {
    /// No host mapping was configured.
    #[error("at least one tenant host mapping is required")]
    EmptyMapping,
    /// One configured authority was blank or structurally invalid.
    #[error("tenant host mapping must be one valid HTTP authority without userinfo")]
    InvalidHost,
    /// More than one configured mapping normalized to the same authority.
    #[error("tenant host mapping contains a duplicate authority")]
    DuplicateHost,
}

fn canonical_host(host: &str) -> Result<String, HostTenantResolverError> {
    if host.trim().is_empty() || host.contains([' ', '\"', '@']) {
        return Err(HostTenantResolverError::InvalidHost);
    }
    host.parse::<http::uri::Authority>()
        .map(|authority| authority.as_str().to_ascii_lowercase())
        .map_err(|_| HostTenantResolverError::InvalidHost)
}

fn request_host(headers: &HeaderMap) -> Option<String> {
    let values = headers.get_all(HOST);
    let mut values = values.iter();
    let host = values.next()?;
    if values.next().is_some() {
        return None;
    }
    host.to_str()
        .ok()
        .and_then(|host| canonical_host(host).ok())
}

/// A layer that resolves a trusted tenant, checks it against the principal, and inserts it.
///
/// Place this layer inside `AuthLayer` so the resolver receives a verified [`Principal`]. It
/// returns 404 for an unmapped tenant, 403 for a principal mismatch, and a sanitized 503 when its
/// resolver fails.
#[derive(Clone)]
#[must_use = "a tenant resolution layer must be applied to a service to have an effect"]
pub struct TenantResolutionLayer<R> {
    resolver: R,
}

impl<R> TenantResolutionLayer<R> {
    /// Creates a tenant-resolution boundary from a trusted resolver.
    pub const fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R> fmt::Debug for TenantResolutionLayer<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantResolutionLayer")
            .field("resolver", &std::any::type_name::<R>())
            .finish()
    }
}

/// Service produced by [`TenantResolutionLayer`].
#[derive(Clone)]
pub struct TenantResolution<R> {
    inner: BoxCloneService<Request, Response, Infallible>,
    resolver: R,
}

impl<R> fmt::Debug for TenantResolution<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantResolution")
            .field("resolver", &std::any::type_name::<R>())
            .finish_non_exhaustive()
    }
}

impl<S, R> Layer<S> for TenantResolutionLayer<R>
where
    R: TenantResolver,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = TenantResolution<R>;

    fn layer(&self, inner: S) -> Self::Service {
        TenantResolution {
            inner: BoxCloneService::new(inner),
            resolver: self.resolver.clone(),
        }
    }
}

impl<R> Service<Request> for TenantResolution<R>
where
    R: TenantResolver,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let resolver = self.resolver.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>().cloned() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            let context = match resolver.resolve(&request, &principal).await {
                Ok(Some(context)) => context,
                Ok(None) => {
                    return Ok(Error::new(
                        StatusCode::NOT_FOUND,
                        "tenant_not_found",
                        "the requested tenant is not available",
                    )
                    .into_response());
                }
                Err(_) => {
                    return Ok(Error::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "tenant_resolution_unavailable",
                        "tenant resolution is unavailable",
                    )
                    .into_response());
                }
            };
            if principal.tenant() != Some(context.tenant()) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "tenant_mismatch",
                    "the authenticated principal does not belong to this tenant",
                )
                .into_response());
            }
            request.extensions_mut().insert(context);
            inner.call_ready(request).await
        })
    }
}

/// A layer that requires the authenticated principal to match request [`TenantContext`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use = "a tenant policy must be applied to a service to have an effect"]
pub struct RequireTenantMatchLayer;

impl RequireTenantMatchLayer {
    /// Creates a tenant isolation layer.
    pub const fn new() -> Self {
        Self
    }
}

/// Service produced by [`RequireTenantMatchLayer`].
#[derive(Clone, Debug)]
pub struct RequireTenantMatch {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for RequireTenantMatchLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequireTenantMatch;

    fn layer(&self, inner: S) -> Self::Service {
        RequireTenantMatch {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for RequireTenantMatch {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            let Some(context) = request.extensions().get::<TenantContext>() else {
                return Ok(Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "tenant_context_missing",
                    "tenant context is required for this route",
                )
                .into_response());
            };
            if principal.tenant() != Some(context.tenant()) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "tenant_mismatch",
                    "the authenticated principal does not belong to this tenant",
                )
                .into_response());
            }
            inner.call_ready(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{HostTenantResolver, TenantContext};

    #[test]
    fn host_tenant_resolver_debug_redacts_host_and_tenant_identifiers() {
        let resolver = HostTenantResolver::new([(
            "private.example.test",
            TenantContext::new("private-tenant").unwrap(),
        )])
        .unwrap();

        let debug = format!("{resolver:?}");
        assert!(debug.contains("configured_host_count: 1"));
        assert!(!debug.contains("private.example.test"));
        assert!(!debug.contains("private-tenant"));
    }
}
