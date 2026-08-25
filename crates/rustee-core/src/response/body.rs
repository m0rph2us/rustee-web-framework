//! Owned HTTP body aliases and transport-neutral body construction.

use std::{convert::Infallible, error::Error as StdError};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};

/// Error type carried by request and response bodies.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Rustee's owned, streaming-compatible HTTP body.
pub type Body = UnsyncBoxBody<Bytes, BoxError>;

/// Rustee's request type.
pub type Request = HttpRequest<Body>;

/// Rustee's response type.
pub type Response = HttpResponse<Body>;

/// Constructs a body from owned bytes.
#[must_use]
pub fn full_body(value: impl Into<Bytes>) -> Body {
    Full::new(value.into())
        .map_err(|never: Infallible| -> BoxError { match never {} })
        .boxed_unsync()
}

/// Constructs an empty body.
#[must_use]
pub fn empty_body() -> Body {
    full_body(Bytes::new())
}

/// Constructs a streaming body from fallible byte chunks.
///
/// The stream is consumed only while the HTTP response body is being sent. Dropping the response
/// drops the stream, allowing upstream producers to observe normal request cancellation.
#[must_use]
pub fn stream_body<S, E>(stream: S) -> Body
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Send + 'static,
    E: Into<BoxError> + 'static,
{
    BodyExt::boxed_unsync(StreamBody::new(
        stream.map(|chunk| chunk.map(Frame::data).map_err(Into::into)),
    ))
}

/// Constructs an HTTP response with a status and body.
///
/// Informational, 204, 205, and 304 statuses cannot carry a payload body, so their supplied body
/// is replaced with an empty one.
#[must_use]
pub fn response(status: StatusCode, body: Body) -> Response {
    let body = status_disallows_body(status)
        .then(empty_body)
        .unwrap_or(body);
    let mut response = HttpResponse::new(body);
    *response.status_mut() = status;
    response
}

pub(super) fn status_disallows_body(status: StatusCode) -> bool {
    status.is_informational()
        || matches!(
            status,
            StatusCode::NO_CONTENT | StatusCode::RESET_CONTENT | StatusCode::NOT_MODIFIED
        )
}
