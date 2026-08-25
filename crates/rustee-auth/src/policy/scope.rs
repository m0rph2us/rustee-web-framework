use std::{
    collections::BTreeSet,
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::exceeds_principal_authorization_value_limit;
use crate::{
    AuthError, MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, MAX_PRINCIPAL_AUTHORIZATION_VALUES,
    Principal, bearer::authentication_response,
};

/// A layer that requires all configured scopes from an authenticated principal.
#[derive(Clone, Eq, PartialEq)]
#[must_use = "a scope policy must be applied to a service to have an effect"]
pub struct RequireScopesLayer {
    required: BTreeSet<String>,
}

impl fmt::Debug for RequireScopesLayer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequireScopesLayer")
            .field("required_scope_count", &self.required.len())
            .finish()
    }
}

impl RequireScopesLayer {
    /// Creates a policy that requires every supplied scope.
    ///
    /// # Errors
    ///
    /// Returns [`ScopePolicyError`] for an empty, blank, oversized, or impossible scope
    /// requirement.
    pub fn new(
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, ScopePolicyError> {
        let mut required = BTreeSet::new();
        for scope in scopes {
            let scope = scope.into();
            if !required.contains(&scope) && required.len() == MAX_PRINCIPAL_AUTHORIZATION_VALUES {
                return Err(ScopePolicyError::TooManyScopes {
                    max_values: MAX_PRINCIPAL_AUTHORIZATION_VALUES,
                });
            }
            required.insert(scope);
        }
        if required.is_empty() {
            return Err(ScopePolicyError::EmptyRequirement);
        }
        if required.iter().any(|scope| scope.trim().is_empty()) {
            return Err(ScopePolicyError::BlankScope);
        }
        if required
            .iter()
            .any(|scope| exceeds_principal_authorization_value_limit(scope))
        {
            return Err(ScopePolicyError::ScopeTooLong {
                max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
            });
        }
        Ok(Self { required })
    }
}

/// Invalid scope policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ScopePolicyError {
    /// No scopes were required.
    #[error("a scope policy must require at least one scope")]
    EmptyRequirement,
    /// A supplied scope was blank.
    #[error("a required scope must not be blank")]
    BlankScope,
    /// A supplied scope cannot be held by a principal.
    #[error("a required scope exceeds the {max_bytes}-byte principal limit")]
    ScopeTooLong {
        /// The shared principal authorization-value limit.
        max_bytes: usize,
    },
    /// A principal cannot hold every configured required scope.
    #[error("a scope policy cannot require more than {max_values} distinct scopes")]
    TooManyScopes {
        /// The shared principal authorization-set limit.
        max_values: usize,
    },
}

/// Service produced by [`RequireScopesLayer`].
#[derive(Clone)]
pub struct RequireScopes {
    inner: BoxCloneService<Request, Response, Infallible>,
    required: BTreeSet<String>,
}

impl fmt::Debug for RequireScopes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequireScopes")
            .field("required_scope_count", &self.required.len())
            .finish_non_exhaustive()
    }
}

impl<S> Layer<S> for RequireScopesLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequireScopes;

    fn layer(&self, inner: S) -> Self::Service {
        RequireScopes {
            inner: BoxCloneService::new(inner),
            required: self.required.clone(),
        }
    }
}

impl Service<Request> for RequireScopes {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let required = self.required.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            if !required.iter().all(|scope| principal.has_scope(scope)) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "insufficient_scope",
                    "the authenticated principal lacks a required scope",
                )
                .into_response());
            }
            inner.call_ready(request).await
        })
    }
}
