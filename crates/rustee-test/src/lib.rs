//! Small, bounded in-process helpers for Rustee application tests.
//!
//! [`TestApp`] dispatches directly to [`rustee_router::App`]. An opt-in [`TestCookieJar`] can
//! retain simple response cookies for session-style tests, but it does not model browser origin,
//! path, secure transport, same-site, redirect, HTTP-version, or SSE behavior. Keep those
//! behaviors in focused wire or browser integration tests.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request as HttpRequest, StatusCode, Uri,
    header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
};
use http_body_util::BodyExt;
use rustee_core::{Request, Response, empty_body, full_body};
use rustee_router::App;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Default maximum response body retained by [`TestApp`].
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Maximum number of cookies retained by one [`TestCookieJar`].
pub const DEFAULT_MAX_COOKIE_COUNT: usize = 64;

/// Maximum combined name/value bytes retained by one [`TestCookieJar`].
pub const DEFAULT_MAX_COOKIE_BYTES: usize = 8 * 1024;

/// A cloneable in-process application client for test code.
#[derive(Clone, Debug)]
pub struct TestApp {
    app: App,
    max_response_bytes: usize,
    cookie_jar: Option<TestCookieJar>,
}

impl TestApp {
    /// Creates a client with a 1 MiB response body bound.
    #[must_use]
    pub fn new(app: App) -> Self {
        Self {
            app,
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
        if max_response_bytes == 0 {
            return Err(TestAppError::ZeroResponseBodyLimit);
        }
        Ok(Self {
            app,
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
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed.
    pub fn request(
        &self,
        method: Method,
        uri: impl AsRef<str>,
    ) -> std::result::Result<TestRequest, TestRequestError> {
        let uri = uri
            .as_ref()
            .parse()
            .map_err(|_| TestRequestError::InvalidUri)?;
        Ok(self.request_uri(method, uri))
    }

    /// Starts a request from an already validated URI.
    pub fn request_uri(&self, method: Method, uri: Uri) -> TestRequest {
        TestRequest {
            app: self.app.clone(),
            method,
            uri,
            headers: HeaderMap::new(),
            body: Bytes::new(),
            max_response_bytes: self.max_response_bytes,
            cookie_jar: self.cookie_jar.clone(),
        }
    }

    /// Starts a GET request.
    ///
    /// # Errors
    ///
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed.
    pub fn get(&self, uri: impl AsRef<str>) -> std::result::Result<TestRequest, TestRequestError> {
        self.request(Method::GET, uri)
    }

    /// Starts a POST request.
    ///
    /// # Errors
    ///
    /// Returns [`TestRequestError::InvalidUri`] when the URI is malformed.
    pub fn post(&self, uri: impl AsRef<str>) -> std::result::Result<TestRequest, TestRequestError> {
        self.request(Method::POST, uri)
    }
}

/// A mutable-by-value request declaration for [`TestApp`].
#[derive(Clone, Debug)]
#[must_use = "call send to dispatch a test request"]
pub struct TestRequest {
    app: App,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    max_response_bytes: usize,
    cookie_jar: Option<TestCookieJar>,
}

impl TestRequest {
    /// Adds one validated request header.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when the header name or value is invalid.
    pub fn header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> std::result::Result<Self, TestRequestError> {
        let name =
            HeaderName::try_from(name.as_ref()).map_err(|_| TestRequestError::InvalidHeaderName)?;
        let value = HeaderValue::try_from(value.as_ref())
            .map_err(|_| TestRequestError::InvalidHeaderValue)?;
        self.headers.insert(name, value);
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
    /// Returns a sanitized error when serialization fails.
    pub fn json<T>(mut self, value: &T) -> std::result::Result<Self, TestRequestError>
    where
        T: Serialize,
    {
        self.body = serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|_| TestRequestError::JsonEncoding)?;
        self.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(self)
    }

    /// Dispatches the request and retains at most the configured response body size.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when a response body cannot be read or exceeds the configured
    /// bound.
    pub async fn send(self) -> std::result::Result<TestResponse, TestResponseError> {
        let mut request: Request = HttpRequest::new(full_body(self.body));
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

/// A bounded cookie jar for opt-in [`TestApp`] session-style flows.
///
/// `TestCookieJar` is intentionally not a browser emulator. It retains only valid ASCII cookie
/// name/value pairs, applies response `Max-Age=0` deletion, and emits one request `Cookie`
/// header. Cookie values are never included in its `Debug` output or public inspection API.
#[derive(Clone, Default)]
pub struct TestCookieJar {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
}

impl fmt::Debug for TestCookieJar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestCookieJar")
            .field("entry_count", &self.len())
            .finish()
    }
}

impl TestCookieJar {
    /// Clears all retained cookies.
    pub fn clear(&self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Returns the number of retained cookies without exposing names or values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Returns whether no cookies are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn request_header(&self) -> Option<HeaderValue> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (!entries.is_empty()).then(|| {
            let value = entries
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            HeaderValue::try_from(value)
                .expect("validated test cookie name/value pairs produce a valid header")
        })
    }

    fn absorb(&self, headers: &HeaderMap) -> std::result::Result<(), TestResponseError> {
        let parsed_updates = headers
            .get_all(SET_COOKIE)
            .iter()
            .map(parse_set_cookie)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if parsed_updates.is_empty() {
            return Ok(());
        }

        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next_entries = entries.clone();
        for update in parsed_updates {
            match update {
                CookieUpdate::Set { name, value } => {
                    next_entries.insert(name, value);
                }
                CookieUpdate::Remove { name } => {
                    next_entries.remove(&name);
                }
            }
            if next_entries.len() > DEFAULT_MAX_COOKIE_COUNT
                || next_entries
                    .iter()
                    .map(|(name, value)| name.len() + value.len())
                    .sum::<usize>()
                    > DEFAULT_MAX_COOKIE_BYTES
            {
                return Err(TestResponseError::CookieJarLimitExceeded);
            }
        }
        *entries = next_entries;
        Ok(())
    }
}

enum CookieUpdate {
    Set { name: String, value: String },
    Remove { name: String },
}

fn parse_set_cookie(value: &HeaderValue) -> std::result::Result<CookieUpdate, TestResponseError> {
    let value = value
        .to_str()
        .map_err(|_| TestResponseError::InvalidSetCookie)?;
    let mut attributes = value.split(';');
    let Some((name, cookie_value)) = attributes
        .next()
        .and_then(|pair| pair.trim().split_once('='))
    else {
        return Err(TestResponseError::InvalidSetCookie);
    };
    if !valid_cookie_name(name) || !valid_cookie_value(cookie_value) {
        return Err(TestResponseError::InvalidSetCookie);
    }
    let expires_now = attributes.any(|attribute| {
        attribute
            .trim()
            .split_once('=')
            .is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("max-age") && value.trim() == "0"
            })
    });
    if expires_now {
        Ok(CookieUpdate::Remove {
            name: name.to_owned(),
        })
    } else {
        Ok(CookieUpdate::Set {
            name: name.to_owned(),
            value: cookie_value.to_owned(),
        })
    }
}

fn valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn valid_cookie_value(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte == b'!'
            || (b'#'..=b'+').contains(&byte)
            || (b'-'..=b':').contains(&byte)
            || (b'<'..=b'[').contains(&byte)
            || (b']'..=b'~').contains(&byte)
    })
}

/// A fully buffered, bounded response returned by [`TestRequest::send`].
#[derive(Clone, Debug)]
pub struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl TestResponse {
    async fn from_response(
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

/// Configuration errors for [`TestApp`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestAppError {
    /// The response body bound was zero.
    #[error("test response body limit must be greater than zero")]
    ZeroResponseBodyLimit,
}

/// Request-construction errors for [`TestApp`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TestRequestError {
    /// The request URI was invalid.
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
}

/// Response-read and decode errors for [`TestRequest::send`].
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

#[cfg(test)]
mod tests {
    use http::header::{CONTENT_TYPE, COOKIE, SET_COOKIE};
    use rustee_core::{Json, empty_body, full_body, response};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Greeting {
        name: String,
    }

    #[tokio::test]
    async fn test_app_sends_json_and_decodes_a_bounded_json_response() {
        let app = App::new().post("/greeting", |Json(greeting): Json<Greeting>| async move {
            (
                StatusCode::CREATED,
                Json(Greeting {
                    name: greeting.name,
                }),
            )
        });
        let response = TestApp::new(app)
            .post("/greeting")
            .unwrap()
            .header("x-test-id", "request-1")
            .unwrap()
            .json(&Greeting {
                name: "Ada".to_owned(),
            })
            .unwrap()
            .send()
            .await
            .unwrap();

        response.assert_status(StatusCode::CREATED).unwrap();
        response
            .assert_header(
                &CONTENT_TYPE,
                &HeaderValue::from_static("application/json; charset=utf-8"),
            )
            .unwrap();
        assert_eq!(response.json::<Greeting>().unwrap().name, "Ada");
    }

    #[tokio::test]
    async fn response_bound_stops_before_retaining_an_oversized_body() {
        let app = App::new().get("/large", || async {
            response(StatusCode::OK, full_body(Bytes::from_static(b"oversized")))
        });
        let error = TestApp::with_max_response_bytes(app, 4)
            .unwrap()
            .get("/large")
            .unwrap()
            .send()
            .await
            .unwrap_err();
        assert_eq!(error, TestResponseError::ResponseTooLarge);
    }

    #[test]
    fn request_and_configuration_validation_are_sanitized() {
        assert_eq!(
            TestApp::with_max_response_bytes(App::new(), 0).unwrap_err(),
            TestAppError::ZeroResponseBodyLimit
        );
        assert_eq!(
            TestApp::new(App::new()).get("not a URI").unwrap_err(),
            TestRequestError::InvalidUri
        );
        assert_eq!(
            TestApp::new(App::new())
                .get("/")
                .unwrap()
                .header("bad header", "value")
                .unwrap_err(),
            TestRequestError::InvalidHeaderName
        );
    }

    #[tokio::test]
    async fn assertions_do_not_render_response_body_or_header_values() {
        let app = App::new().get("/", || async {
            let mut response = response(
                StatusCode::ACCEPTED,
                full_body(Bytes::from_static(b"secret")),
            );
            response
                .headers_mut()
                .insert("x-private", HeaderValue::from_static("secret"));
            response
        });
        let response = TestApp::new(app).get("/").unwrap().send().await.unwrap();
        assert_eq!(
            response
                .assert_status(StatusCode::OK)
                .unwrap_err()
                .to_string(),
            "expected HTTP status 200, received 202"
        );
        assert_eq!(
            response
                .assert_header(
                    &HeaderName::from_static("x-private"),
                    &HeaderValue::from_static("different"),
                )
                .unwrap_err()
                .to_string(),
            "response header did not match the expected value"
        );
    }

    #[tokio::test]
    async fn opt_in_cookie_jar_carries_session_style_cookies_and_honors_manual_override() {
        let app = App::new()
            .get("/login", || async {
                let mut response = response(StatusCode::NO_CONTENT, empty_body());
                response.headers_mut().append(
                    SET_COOKIE,
                    HeaderValue::from_static("session=opaque; Path=/; HttpOnly; SameSite=Lax"),
                );
                response
                    .headers_mut()
                    .append(SET_COOKIE, HeaderValue::from_static("theme=dark; Path=/"));
                response
            })
            .get("/profile", |headers: HeaderMap| async move {
                headers
                    .get(COOKIE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("<missing>")
                    .to_owned()
            })
            .get("/logout", || async {
                let mut response = response(StatusCode::NO_CONTENT, empty_body());
                response.headers_mut().append(
                    SET_COOKIE,
                    HeaderValue::from_static("session=; Path=/; Max-Age=0; HttpOnly"),
                );
                response
            });
        let client = TestApp::new(app).with_cookie_jar();
        let jar = client.cookie_jar().unwrap();

        client.get("/login").unwrap().send().await.unwrap();
        assert_eq!(jar.len(), 2);
        assert_eq!(
            client
                .get("/profile")
                .unwrap()
                .send()
                .await
                .unwrap()
                .text()
                .unwrap(),
            "session=opaque; theme=dark"
        );
        assert_eq!(
            client
                .get("/profile")
                .unwrap()
                .header("cookie", "session=manual")
                .unwrap()
                .send()
                .await
                .unwrap()
                .text()
                .unwrap(),
            "session=manual"
        );

        client.get("/logout").unwrap().send().await.unwrap();
        assert_eq!(jar.len(), 1);
        assert_eq!(
            client
                .get("/profile")
                .unwrap()
                .send()
                .await
                .unwrap()
                .text()
                .unwrap(),
            "theme=dark"
        );
        jar.clear();
        assert!(jar.is_empty());
    }

    #[tokio::test]
    async fn cookie_jar_rejects_malformed_set_cookie_without_retaining_it() {
        let app = App::new().get("/invalid", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response
                .headers_mut()
                .append(SET_COOKIE, HeaderValue::from_static("not-a-cookie"));
            response
        });
        let client = TestApp::new(app).with_cookie_jar();
        let jar = client.cookie_jar().unwrap();

        let error = client.get("/invalid").unwrap().send().await.unwrap_err();

        assert_eq!(error, TestResponseError::InvalidSetCookie);
        assert!(jar.is_empty());
    }

    #[tokio::test]
    async fn cookie_jar_rejects_an_over_capacity_response_atomically() {
        let app = App::new().get("/many", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            for index in 0..=DEFAULT_MAX_COOKIE_COUNT {
                response.headers_mut().append(
                    SET_COOKIE,
                    HeaderValue::try_from(format!("cookie{index}=value")).unwrap(),
                );
            }
            response
        });
        let client = TestApp::new(app).with_cookie_jar();
        let jar = client.cookie_jar().unwrap();

        let error = client.get("/many").unwrap().send().await.unwrap_err();

        assert_eq!(error, TestResponseError::CookieJarLimitExceeded);
        assert!(jar.is_empty());
    }

    #[tokio::test]
    async fn cookie_jar_rejects_an_over_byte_bound_response_atomically() {
        let app = App::new().get("/large-cookie", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::try_from(format!("session={}", "x".repeat(DEFAULT_MAX_COOKIE_BYTES)))
                    .unwrap(),
            );
            response
        });
        let client = TestApp::new(app).with_cookie_jar();
        let jar = client.cookie_jar().unwrap();

        let error = client
            .get("/large-cookie")
            .unwrap()
            .send()
            .await
            .unwrap_err();

        assert_eq!(error, TestResponseError::CookieJarLimitExceeded);
        assert!(jar.is_empty());
    }

    #[tokio::test]
    async fn cookie_jar_updates_only_after_the_bounded_response_is_read() {
        let app = App::new().get("/large", || async {
            let mut response =
                response(StatusCode::OK, full_body(Bytes::from_static(b"oversized")));
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::from_static("session=opaque; Path=/"),
            );
            response
        });
        let client = TestApp::with_max_response_bytes(app, 4)
            .unwrap()
            .with_cookie_jar();
        let jar = client.cookie_jar().unwrap();

        let error = client.get("/large").unwrap().send().await.unwrap_err();

        assert_eq!(error, TestResponseError::ResponseTooLarge);
        assert!(jar.is_empty());
    }
}
