//! Typed request extraction and application state.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::{HeaderMap, Method, StatusCode, Uri};
use http_body_util::{BodyExt, LengthLimitError};

use crate::{ConnectionInfo, Error, Request, Result, RouteParams, RouteTemplate};

mod header;
mod json;
mod parameters;
mod state;

pub use header::{FromHeader, Header};
pub use json::Json;
pub use parameters::{Path, Query};
pub use state::{State, StateStore};

/// Extracts a handler argument from an incoming request.
pub trait FromRequest: Sized + Send {
    /// Extracts one value. Extractors run left-to-right in handler signatures.
    fn from_request<'a>(
        request: &'a mut Request,
        params: &'a RouteParams,
        state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>>;
}

impl FromRequest for ConnectionInfo {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            request.extensions().get::<Self>().copied().ok_or_else(|| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "connection_info_missing",
                    "transport connection metadata is required",
                )
            })
        })
    }
}

impl FromRequest for RouteTemplate {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "route_template_missing",
                    "a matched router route is required",
                )
            })
        })
    }
}

impl FromRequest for HeaderMap {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { Ok(request.headers().clone()) })
    }
}

impl FromRequest for Bytes {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { collect_body(request).await })
    }
}

impl FromRequest for Method {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { Ok(request.method().clone()) })
    }
}

impl FromRequest for Uri {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { Ok(request.uri().clone()) })
    }
}

async fn collect_body(request: &mut Request) -> Result<Bytes> {
    request
        .body_mut()
        .collect()
        .await
        .map_err(|error| {
            if error.downcast_ref::<LengthLimitError>().is_some() {
                Error::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    "request body exceeds the configured limit",
                )
            } else {
                Error::bad_request("request body could not be read")
            }
        })
        .map(http_body_util::Collected::to_bytes)
}
