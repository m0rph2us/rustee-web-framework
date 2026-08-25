//! Tower request handling and verified forwarded-context injection.

use std::{
    convert::Infallible,
    fmt,
    net::IpAddr,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{
    BoxCloneServiceExt, ConnectionInfo, Error, FromRequest, IntoResponse, Request, Response,
    RouteParams, StateStore,
};
use tower::{Layer, Service, util::BoxCloneService};

use super::{
    TrustedProxyPolicy,
    forwarded::{parse_forwarded_headers, parse_x_forwarded_headers},
};

/// Forwarded client data verified through an explicitly trusted reverse-proxy chain.
#[derive(Clone, Eq, PartialEq)]
pub struct ForwardedContext {
    client_ip: IpAddr,
    scheme: Option<String>,
    host: Option<String>,
}

impl fmt::Debug for ForwardedContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForwardedContext")
            .field("client_ip", &"[REDACTED]")
            .field("has_scheme", &self.scheme.is_some())
            .field("has_host", &self.host.is_some())
            .finish()
    }
}

impl ForwardedContext {
    pub(super) fn new(client_ip: IpAddr, scheme: Option<String>, host: Option<String>) -> Self {
        Self {
            client_ip,
            scheme,
            host,
        }
    }

    /// Returns the client IP asserted by the trusted direct proxy.
    #[must_use]
    pub const fn client_ip(&self) -> IpAddr {
        self.client_ip
    }

    /// Returns the trusted external scheme when the proxy supplied `proto`.
    #[must_use]
    pub fn scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }

    /// Returns the trusted external host when the proxy supplied `host`.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
}

impl FromRequest for ForwardedContext {
    fn from_request<'a>(
        request: &'a mut Request,
        _: &'a RouteParams,
        _: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forwarded_context_missing",
                    "trusted proxy context is required",
                )
            })
        })
    }
}

/// Layer that normalizes one selected forwarded-header family only when the direct peer is trusted.
#[derive(Clone, Debug)]
#[must_use = "a trusted proxy policy must be layered onto a service to have an effect"]
pub struct TrustedProxyLayer {
    policy: TrustedProxyPolicy,
    header_family: TrustedProxyHeaderFamily,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrustedProxyHeaderFamily {
    Forwarded,
    XForwarded,
}

impl TrustedProxyLayer {
    /// Creates a layer that accepts RFC 7239 `Forwarded` headers from the configured proxies.
    pub const fn new(policy: TrustedProxyPolicy) -> Self {
        Self {
            policy,
            header_family: TrustedProxyHeaderFamily::Forwarded,
        }
    }

    /// Uses the `X-Forwarded-For`/`Proto`/`Host` family instead of RFC 7239 `Forwarded`.
    ///
    /// This is explicit because deployments must configure their direct trusted proxy to sanitize
    /// exactly one forwarded-header family. The same policy chain-hop bound applies.
    pub const fn with_x_forwarded(mut self) -> Self {
        self.header_family = TrustedProxyHeaderFamily::XForwarded;
        self
    }
}

/// Service produced by [`TrustedProxyLayer`] that normalizes trusted forwarded headers.
#[derive(Clone, Debug)]
pub struct TrustedProxy {
    inner: BoxCloneService<Request, Response, Infallible>,
    policy: TrustedProxyPolicy,
    header_family: TrustedProxyHeaderFamily,
}

impl<S> Layer<S> for TrustedProxyLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = TrustedProxy;

    fn layer(&self, inner: S) -> Self::Service {
        TrustedProxy {
            inner: BoxCloneService::new(inner),
            policy: self.policy.clone(),
            header_family: self.header_family,
        }
    }
}

impl Service<Request> for TrustedProxy {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let inner = self.inner.clone();
        let policy = self.policy.clone();
        let header_family = self.header_family;
        Box::pin(async move {
            let peer = request
                .extensions()
                .get::<ConnectionInfo>()
                .copied()
                .ok_or_else(|| {
                    Error::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "connection_info_missing",
                        "transport connection metadata is required",
                    )
                    .into_response()
                });
            let peer = match peer {
                Ok(peer) => peer,
                Err(response) => return Ok(response),
            };
            if policy.trusts(peer.peer_addr().ip()) {
                let context = match header_family {
                    TrustedProxyHeaderFamily::Forwarded => {
                        parse_forwarded_headers(request.headers(), &policy)
                    }
                    TrustedProxyHeaderFamily::XForwarded => {
                        parse_x_forwarded_headers(request.headers(), &policy)
                    }
                };
                match context {
                    Ok(Some(context)) => {
                        request.extensions_mut().insert(context);
                    }
                    Ok(None) => {}
                    Err(()) => {
                        return Ok(Error::new(
                            StatusCode::BAD_REQUEST,
                            "invalid_forwarded_header",
                            "the trusted proxy forwarded header is invalid",
                        )
                        .into_response());
                    }
                }
            }
            inner.call_ready(request).await
        })
    }
}
