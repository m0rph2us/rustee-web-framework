//! Sanitized errors and handler response conversions.

use std::{error::Error as StdError, fmt};

use bytes::Bytes;
use http::{
    HeaderValue, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING},
};
use serde::Serialize;

use crate::Json;

mod body;
mod json;

pub use body::{Body, BoxError, Request, Response, empty_body, full_body, response, stream_body};
pub use json::{json_response, json_response_bounded};

/// Convenience result alias for application and extractor errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A sanitized error that can be rendered as an HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl Error {
    /// Creates an error with an explicit status, stable code, and public message.
    #[must_use]
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    /// Creates a bad request error.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    /// Creates a not-found error.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    /// Creates an unsupported-media-type error.
    #[must_use]
    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            message,
        )
    }

    /// Creates a safe internal-server-error response.
    #[must_use]
    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "an internal server error occurred",
        )
    }

    /// Returns the corresponding HTTP status.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the stable, machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl StdError for Error {}

#[derive(Serialize)]
struct ErrorPayload<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

/// Converts values returned by handlers into an HTTP response.
pub trait IntoResponse {
    /// Produces a complete HTTP response.
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let payload = ErrorPayload {
            error: ErrorBody {
                code: self.code,
                message: &self.message,
            },
        };
        json_response(self.status, &payload).unwrap_or_else(|_| response(self.status, empty_body()))
    }
}

impl IntoResponse for StatusCode {
    fn into_response(self) -> Response {
        response(self, empty_body())
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Response {
        StatusCode::NO_CONTENT.into_response()
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response {
        text_response(StatusCode::OK, self)
    }
}

impl IntoResponse for &'static str {
    fn into_response(self) -> Response {
        text_response(StatusCode::OK, self)
    }
}

impl IntoResponse for Bytes {
    fn into_response(self) -> Response {
        response(StatusCode::OK, full_body(self))
    }
}

impl IntoResponse for Vec<u8> {
    fn into_response(self) -> Response {
        Bytes::from(self).into_response()
    }
}

impl<T> IntoResponse for (StatusCode, T)
where
    T: IntoResponse,
{
    fn into_response(self) -> Response {
        let (status, value) = self;
        let mut response = value.into_response();
        *response.status_mut() = status;
        if body::status_disallows_body(status) {
            *response.body_mut() = empty_body();
            response.headers_mut().remove(CONTENT_LENGTH);
            response.headers_mut().remove(TRANSFER_ENCODING);
        }
        response
    }
}

impl<T, E> IntoResponse for std::result::Result<T, E>
where
    T: IntoResponse,
    E: IntoResponse,
{
    fn into_response(self) -> Response {
        match self {
            Ok(value) => value.into_response(),
            Err(error) => error.into_response(),
        }
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        json_response(StatusCode::OK, &self.0).unwrap_or_else(|_| Error::internal().into_response())
    }
}

fn text_response(status: StatusCode, value: impl Into<Bytes>) -> Response {
    let mut response = response(status, full_body(value));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}
