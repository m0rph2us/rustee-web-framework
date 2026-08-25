//! Authenticated-principal request extractors.

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{Error, FromRequest, Request, RouteParams, StateStore};

use crate::Principal;

/// Extracts the authenticated principal or returns a 401 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthUser(pub Principal);

impl FromRequest for AuthUser {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { required_principal(request).map(Self) })
    }
}

/// Extracts an authenticated principal with an explicit route-level requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequireAuth(pub Principal);

impl FromRequest for RequireAuth {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { required_principal(request).map(Self) })
    }
}

/// Extracts an authenticated principal when present without requiring one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionalAuthUser(pub Option<Principal>);

impl FromRequest for OptionalAuthUser {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { Ok(Self(request.extensions().get::<Principal>().cloned())) })
    }
}

fn required_principal(request: &Request) -> rustee_core::Result<Principal> {
    request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| {
            Error::new(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "authentication is required",
            )
        })
}
