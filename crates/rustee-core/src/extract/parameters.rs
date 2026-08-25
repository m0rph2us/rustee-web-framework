//! Query and route-parameter extraction with strict decoding boundaries.

use futures_util::future::BoxFuture;
use percent_encoding::percent_decode_str;
use serde::de::DeserializeOwned;

use crate::{Error, Request, Result, RouteParams};

use super::{FromRequest, StateStore};

/// URI query-string request extractor.
///
/// Decode failures return a fixed public bad-request message, never deserializer details.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Query<T>(pub T);

impl<T> std::fmt::Debug for Query<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Query")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

impl<T> FromRequest for Query<T>
where
    T: DeserializeOwned + Send,
{
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            serde_urlencoded::from_str(request.uri().query().unwrap_or_default())
                .map(Self)
                .map_err(|_| Error::bad_request("invalid query string"))
        })
    }
}

/// Named route-parameter request extractor.
///
/// Percent-encoded segments are decoded as UTF-8 before deserialization; literal `+` characters
/// remain literal. Decode failures return a fixed public bad-request message, never deserializer
/// details.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Path<T>(pub T);

impl<T> std::fmt::Debug for Path<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Path")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

impl<T> FromRequest for Path<T>
where
    T: DeserializeOwned + Send,
{
    fn from_request<'a>(
        _request: &'a mut Request,
        params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            for (key, value) in params.iter() {
                let value = decode_path_parameter(value)?;
                serializer.append_pair(key, &value);
            }
            serde_urlencoded::from_str(&serializer.finish())
                .map(Self)
                .map_err(|_| Error::bad_request("invalid path parameters"))
        })
    }
}

fn decode_path_parameter(value: &str) -> Result<String> {
    if !has_valid_percent_encoding(value) {
        return Err(Error::bad_request("invalid path parameters"));
    }
    percent_decode_str(value)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| Error::bad_request("invalid path parameters"))
}

fn has_valid_percent_encoding(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}
