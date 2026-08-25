//! Single-value typed header extraction and redacted wrapper diagnostics.

use futures_util::future::BoxFuture;
use http::{HeaderValue, header::HeaderName};

use crate::{Error, Request, Result, RouteParams};

use super::{FromRequest, StateStore};

/// A typed single-value request-header extractor.
///
/// [`Header`] rejects duplicate field values before invoking [`FromHeader`]. Use
/// [`http::HeaderMap`] when an endpoint deliberately needs to interpret a multi-value HTTP field.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Header<T>(pub T);

impl<T> std::fmt::Debug for Header<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Header")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Implement this trait to parse one strongly typed request header value.
///
/// [`Header`] calls this only when the declared field occurs exactly once.
pub trait FromHeader: Sized {
    /// Header name to read.
    const NAME: &'static str;

    /// Parses a header value into the requested type.
    ///
    /// # Errors
    ///
    /// Returns an error when the header value does not satisfy the type's contract.
    fn from_header(value: &HeaderValue) -> Result<Self>;
}

impl<T> FromRequest for Header<T>
where
    T: FromHeader + Send,
{
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move {
            let name = HeaderName::from_bytes(T::NAME.as_bytes()).map_err(|_| Error::internal())?;
            let mut values = request.headers().get_all(&name).iter();
            let value = values
                .next()
                .ok_or_else(|| Error::bad_request(format!("missing {} header", T::NAME)))?;
            if values.next().is_some() {
                return Err(Error::bad_request(format!("duplicate {} header", T::NAME)));
            }
            T::from_header(value).map(Self)
        })
    }
}
