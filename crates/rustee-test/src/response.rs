use std::fmt;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::SET_COOKIE};
use http_body_util::BodyExt;
use rustee_core::Response;
use serde::de::DeserializeOwned;
use thiserror::Error;

/// A fully buffered, bounded response returned by [`crate::TestRequest::send`].
///
/// Its `Debug` output reports only status and response shape, never headers or body content.
#[derive(Clone)]
pub struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl fmt::Debug for TestResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("has_set_cookie", &self.headers.contains_key(SET_COOKIE))
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl TestResponse {
    pub(crate) async fn from_response(
        response: Response,
        max_response_bytes: usize,
    ) -> std::result::Result<Self, TestResponseError> {
        let (parts, mut body) = response.into_parts();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| TestResponseError::BodyRead)?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let remaining = max_response_bytes.saturating_sub(bytes.len());
            if data.len() > remaining {
                return Err(TestResponseError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&data);
        }
        Ok(Self {
            status: parts.status,
            headers: parts.headers,
            body: Bytes::from(bytes),
        })
    }

    /// Returns the HTTP status.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns all response headers.
    #[must_use]
    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns one response header by name.
    #[must_use]
    pub fn header(&self, name: &HeaderName) -> Option<&HeaderValue> {
        self.headers.get(name)
    }

    /// Returns the buffered response body.
    #[must_use]
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Decodes the buffered response as UTF-8 text.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when the body is not UTF-8.
    pub fn text(&self) -> std::result::Result<&str, TestResponseError> {
        std::str::from_utf8(&self.body).map_err(|_| TestResponseError::InvalidUtf8)
    }

    /// Decodes the buffered response as JSON.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when the body does not match `T`.
    pub fn json<T>(&self) -> std::result::Result<T, TestResponseError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(&self.body).map_err(|_| TestResponseError::InvalidJson)
    }

    /// Asserts one expected HTTP status without rendering the response body.
    ///
    /// # Errors
    ///
    /// Returns an error with only expected and actual status codes when they differ.
    pub fn assert_status(
        &self,
        expected: StatusCode,
    ) -> std::result::Result<(), TestAssertionError> {
        if self.status == expected {
            Ok(())
        } else {
            Err(TestAssertionError::UnexpectedStatus {
                expected: expected.as_u16(),
                actual: self.status.as_u16(),
            })
        }
    }

    /// Asserts one exact response header without rendering header values on failure.
    ///
    /// # Errors
    ///
    /// Returns a sanitized assertion error when the header is absent or differs.
    pub fn assert_header(
        &self,
        name: &HeaderName,
        expected: &HeaderValue,
    ) -> std::result::Result<(), TestAssertionError> {
        match self.headers.get(name) {
            Some(actual) if actual == expected => Ok(()),
            Some(_) => Err(TestAssertionError::HeaderMismatch),
            None => Err(TestAssertionError::MissingHeader),
        }
    }
}

/// Response-read and decode errors for [`crate::TestRequest::send`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestResponseError {
    /// The response body stream failed.
    #[error("test response body could not be read")]
    BodyRead,
    /// The response body exceeded the configured bound.
    #[error("test response body exceeded its configured limit")]
    ResponseTooLarge,
    /// The response body was not UTF-8.
    #[error("test response body was not valid UTF-8")]
    InvalidUtf8,
    /// The response body was not valid JSON for the requested type.
    #[error("test response body was not valid JSON")]
    InvalidJson,
    /// A response `Set-Cookie` header is outside the test cookie jar's simple syntax.
    #[error("test response set-cookie header is invalid")]
    InvalidSetCookie,
    /// Retaining response cookies exceeded the test cookie jar's fixed bound.
    #[error("test cookie jar exceeded its configured limit")]
    CookieJarLimitExceeded,
}

/// Non-body-rendering assertion failures for [`TestResponse`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestAssertionError {
    /// The response status differed from the expected status.
    #[error("expected HTTP status {expected}, received {actual}")]
    UnexpectedStatus {
        /// Expected status code.
        expected: u16,
        /// Actual status code.
        actual: u16,
    },
    /// A response header was absent.
    #[error("expected response header was missing")]
    MissingHeader,
    /// A response header differed without exposing its value.
    #[error("response header did not match the expected value")]
    HeaderMismatch,
}
