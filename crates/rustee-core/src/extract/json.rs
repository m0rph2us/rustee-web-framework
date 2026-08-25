//! JSON request-body extraction and redacted wrapper diagnostics.

use futures_util::future::BoxFuture;
use http::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;

use crate::{Error, Request, Result, RouteParams, is_standard_json_media_type};

use super::{FromRequest, StateStore, collect_body};

/// JSON request extractor and response wrapper.
///
/// Decode failures return a fixed public bad-request message, never deserializer details.
///
/// The extractor accepts `application/json` and standard `application/*+json` media types with
/// optional parameters. It requires exactly one `Content-Type` field value.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Json<T>(pub T);

impl<T> std::fmt::Debug for Json<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Json")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

impl<T> FromRequest for Json<T>
where
    T: DeserializeOwned + Send,
{
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            let mut content_types = request.headers().get_all(CONTENT_TYPE).iter();
            let Some(content_type) = content_types.next() else {
                return Err(Error::unsupported_media_type(
                    "expected an application/json content type",
                ));
            };
            if content_types.next().is_some() {
                return Err(Error::bad_request("duplicate Content-Type header"));
            }
            let content_type = content_type.to_str().unwrap_or_default();
            if !is_standard_json_media_type(content_type) {
                return Err(Error::unsupported_media_type(
                    "expected an application/json content type",
                ));
            }

            let bytes = collect_body(request).await?;
            serde_json::from_slice(&bytes)
                .map(Self)
                .map_err(|_| Error::bad_request("invalid JSON body"))
        })
    }
}
