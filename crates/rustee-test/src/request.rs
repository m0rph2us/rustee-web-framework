//! In-process request construction and application dispatch.

use std::fmt;

use bytes::Bytes;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request as HttpRequest, Uri,
    header::{AUTHORIZATION, CONTENT_TYPE, COOKIE},
};
use http_body_util::{BodyExt, Limited};
use rustee_core::{Request, Response, empty_body, full_body};
use rustee_json::{BoundedJsonError, to_vec_bounded};
use rustee_router::App;
use serde::Serialize;
use thiserror::Error;

use crate::{TestCookieJar, TestResponse, TestResponseError};

/// Default maximum response body retained by [`TestApp`].
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Default maximum request body delivered by [`TestApp`].
///
/// This matches Rustee server's default HTTP request body limit so in-process extractor tests
/// exercise the same limit behavior as ordinary server deployments.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// A cloneable in-process application client for test code.
#[derive(Clone, Debug)]
pub struct TestApp {
    app: App,
    max_request_bytes: usize,
    max_response_bytes: usize,
    cookie_jar: Option<TestCookieJar>,
}

impl TestApp {
    /// Creates a client with Rustee's default 2 MiB request and 1 MiB retained-response bounds.
    #[must_use]
    pub fn new(app: App) -> Self {
        Self {
            app,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            cookie_jar: None,
        }
    }

    /// Creates a client with an explicit response body bound.
    ///
    /// # Errors
    ///
    /// Returns [`TestAppError::ZeroResponseBodyLimit`] when `max_response_bytes` is zero.
    pub fn with_max_response_bytes(
        app: App,
        max_response_bytes: usize,
    ) -> std::result::Result<Self, TestAppError> {
        Self::with_limits(app, DEFAULT_MAX_REQUEST_BYTES, max_response_bytes)
    }

    /// Creates a client with an explicit request body bound and the default retained-response
    /// bound.
    ///
    /// # Errors
    ///
    /// Returns [`TestAppError::ZeroRequestBodyLimit`] when `max_request_bytes` is zero.
    pub fn with_max_request_bytes(
        app: App,
        max_request_bytes: usize,
    ) -> std::result::Result<Self, TestAppError> {
        Self::with_limits(app, max_request_bytes, DEFAULT_MAX_RESPONSE_BYTES)
    }

    /// Creates a client with explicit request and retained-response body bounds.
    ///
    /// # Errors
    ///
    /// Returns [`TestAppError`] when either limit is zero.
    pub fn with_limits(
        app: App,
        max_request_bytes: usize,
        max_response_bytes: usize,
    ) -> std::result::Result<Self, TestAppError> {
        if max_request_bytes == 0 {
            return Err(TestAppError::ZeroRequestBodyLimit);
        }
        if max_response_bytes == 0 {
            return Err(TestAppError::ZeroResponseBodyLimit);
        }
        Ok(Self {
            app,
            max_request_bytes,
            max_response_bytes,
            cookie_jar: None,
        })
    }

    /// Enables a bounded, shared cookie jar for session-style in-process tests.
    ///
    /// The jar stores only simple cookie name/value pairs from response `Set-Cookie` headers.
    /// It applies `Max-Age=0` deletion but does not evaluate browser origin, path, secure
    /// transport, same-site, expiry, or redirect policy. An explicitly supplied request `Cookie`
    /// header takes precedence over the retained jar.
    #[must_use]
    pub fn with_cookie_jar(mut self) -> Self {
        self.cookie_jar = Some(TestCookieJar::default());
        self
    }

    /// Returns the opt-in cookie jar, when this client has one.
    #[must_use]
    pub fn cookie_jar(&self) -> Option<TestCookieJar> {
        self.cookie_jar.clone()
    }

    /// Starts a request from an absolute-path URI string.
    ///
    /// # Errors
    ///
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed or is not an absolute
    /// path.
    pub fn request(
        &self,
        method: Method,
        uri: impl AsRef<str>,
    ) -> std::result::Result<TestRequest, TestRequestError> {
        let uri = uri
            .as_ref()
            .parse()
            .map_err(|_| TestRequestError::InvalidUri)?;
        if !is_absolute_path_uri(&uri) {
            return Err(TestRequestError::InvalidUri);
        }
        Ok(self.request_uri(method, uri))
    }

    /// Starts a request from a caller-validated absolute-path URI.
    pub fn request_uri(&self, method: Method, uri: Uri) -> TestRequest {
        TestRequest {
            app: self.app.clone(),
            method,
            uri,
            headers: HeaderMap::new(),
            body: Bytes::new(),
            max_request_bytes: self.max_request_bytes,
            max_response_bytes: self.max_response_bytes,
            cookie_jar: self.cookie_jar.clone(),
        }
    }

    /// Starts a GET request.
    ///
    /// # Errors
    ///
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed or is not an absolute
    /// path.
    pub fn get(&self, uri: impl AsRef<str>) -> std::result::Result<TestRequest, TestRequestError> {
        self.request(Method::GET, uri)
    }

    /// Starts a POST request.
    ///
    /// # Errors
    ///
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed or is not an absolute
    /// path.
    pub fn post(&self, uri: impl AsRef<str>) -> std::result::Result<TestRequest, TestRequestError> {
        self.request(Method::POST, uri)
    }
}

/// A mutable-by-value request declaration for [`TestApp`].
///
/// Its `Debug` output reports only request shape and sizes, never the URI, headers, or body.
#[derive(Clone)]
#[must_use = "call send to dispatch a test request"]
pub struct TestRequest {
    app: App,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    max_request_bytes: usize,
    max_response_bytes: usize,
    cookie_jar: Option<TestCookieJar>,
}

impl fmt::Debug for TestRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestRequest")
            .field("method", &self.method)
            .field("has_query", &self.uri.query().is_some())
            .field("header_count", &self.headers.len())
            .field(
                "has_authorization",
                &self.headers.contains_key(AUTHORIZATION),
            )
            .field("has_cookie", &self.headers.contains_key(COOKIE))
            .field("body_len", &self.body.len())
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("has_cookie_jar", &self.cookie_jar.is_some())
            .finish_non_exhaustive()
    }
}

impl TestRequest {
    /// Sets one validated request header, replacing any existing values with the same name.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when the header name or value is invalid.
    pub fn header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> std::result::Result<Self, TestRequestError> {
        let (name, value) = validated_header(name.as_ref(), value.as_ref())?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Appends one validated request header value without removing existing values.
    ///
    /// Use this when a test deliberately needs a repeated HTTP field, such as to verify that an
    /// endpoint rejects ambiguous single-value headers.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when the header name or value is invalid.
    pub fn append_header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> std::result::Result<Self, TestRequestError> {
        let (name, value) = validated_header(name.as_ref(), value.as_ref())?;
        self.headers.append(name, value);
        Ok(self)
    }

    /// Replaces the complete request body without setting a content type.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Serializes a value as a JSON request and sets <code>Content-Type: application/json</code>.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when serialization fails or the encoded JSON exceeds the
    /// configured request body limit.
    pub fn json<T>(mut self, value: &T) -> std::result::Result<Self, TestRequestError>
    where
        T: Serialize,
    {
        self.body = encode_json_bounded(value, self.max_request_bytes)?;
        self.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(self)
    }

    /// Dispatches the request through its configured body limit and retains at most the configured
    /// response body size.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when a response body cannot be read or exceeds the configured
    /// bound.
    pub async fn send(self) -> std::result::Result<TestResponse, TestResponseError> {
        let body = Limited::new(full_body(self.body), self.max_request_bytes).boxed_unsync();
        let mut request: Request = HttpRequest::new(body);
        *request.method_mut() = self.method;
        *request.uri_mut() = self.uri;
        let mut headers = self.headers;
        if !headers.contains_key(COOKIE)
            && let Some(jar) = &self.cookie_jar
            && let Some(cookie) = jar.request_header()
        {
            headers.insert(COOKIE, cookie);
        }
        *request.headers_mut() = headers;
        let response = self.app.call(request).await;
        let response = TestResponse::from_response(response, self.max_response_bytes).await?;
        if let Some(jar) = &self.cookie_jar {
            jar.absorb(response.headers())?;
        }
        Ok(response)
    }
}

/// Configuration errors for [`TestApp`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestAppError {
    /// The request body bound was zero.
    #[error("test request body limit must be greater than zero")]
    ZeroRequestBodyLimit,
    /// The response body bound was zero.
    #[error("test response body limit must be greater than zero")]
    ZeroResponseBodyLimit,
}

/// Request-construction errors for [`TestApp`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestRequestError {
    /// The request URI was malformed or was not an absolute path.
    #[error("test request URI is invalid")]
    InvalidUri,
    /// The header name was invalid.
    #[error("test request header name is invalid")]
    InvalidHeaderName,
    /// The header value was invalid.
    #[error("test request header value is invalid")]
    InvalidHeaderValue,
    /// JSON request serialization failed.
    #[error("test JSON request could not be encoded")]
    JsonEncoding,
    /// JSON request serialization exceeded the configured request-body bound.
    #[error("test JSON request exceeded its configured body limit")]
    JsonTooLarge,
}

fn is_absolute_path_uri(uri: &Uri) -> bool {
    uri.scheme().is_none() && uri.authority().is_none() && uri.path().starts_with('/')
}

fn validated_header(
    name: &str,
    value: &str,
) -> std::result::Result<(HeaderName, HeaderValue), TestRequestError> {
    let name = HeaderName::try_from(name).map_err(|_| TestRequestError::InvalidHeaderName)?;
    let value = HeaderValue::try_from(value).map_err(|_| TestRequestError::InvalidHeaderValue)?;
    Ok((name, value))
}

fn encode_json_bounded<T>(
    value: &T,
    max_bytes: usize,
) -> std::result::Result<Bytes, TestRequestError>
where
    T: Serialize,
{
    match to_vec_bounded(value, max_bytes) {
        Ok(encoded) => Ok(Bytes::from(encoded)),
        Err(BoundedJsonError::TooLarge) => Err(TestRequestError::JsonTooLarge),
        Err(BoundedJsonError::Serialize(_)) => Err(TestRequestError::JsonEncoding),
    }
}

/// Builds and dispatches an empty request through an application.
///
/// Prefer [`TestApp`] for bounded response decoding and request construction. This compatibility
/// helper returns the streaming response unchanged.
///
/// # Panics
///
/// Panics when the supplied `Uri` cannot be used by an HTTP request builder.
pub async fn request(app: &App, method: Method, uri: Uri) -> Response {
    let request: Request = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .body(empty_body())
        .expect("test request URI must be valid");
    app.call(request).await
}
