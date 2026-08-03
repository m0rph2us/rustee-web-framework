//! Stable, transport-independent primitives used by every Rustee crate.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    convert::Infallible,
    error::Error as StdError,
    fmt,
    net::SocketAddr,
    sync::Arc,
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, future::BoxFuture};
use http::{
    HeaderMap, HeaderValue, Method, Request as HttpRequest, Response as HttpResponse, StatusCode,
    Uri,
    header::{CONTENT_TYPE, HeaderName},
};
use http_body::Frame;
use http_body_util::{BodyExt, Full, LengthLimitError, StreamBody, combinators::UnsyncBoxBody};
use serde::{Serialize, de::DeserializeOwned};

/// Error type carried by request and response bodies.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Rustee's owned, streaming-compatible HTTP body.
pub type Body = UnsyncBoxBody<Bytes, BoxError>;

/// Rustee's request type.
pub type Request = HttpRequest<Body>;

/// Rustee's response type.
pub type Response = HttpResponse<Body>;

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

/// JSON request extractor and response wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Json<T>(pub T);

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        json_response(StatusCode::OK, &self.0).unwrap_or_else(|_| Error::internal().into_response())
    }
}

/// URI query-string request extractor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Query<T>(pub T);

/// Named route-parameter request extractor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Path<T>(pub T);

/// Shared application state request extractor.
#[derive(Clone, Debug)]
pub struct State<T>(pub Arc<T>);

/// A typed request-header extractor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header<T>(pub T);

/// Transport-provided connection metadata for the current request.
///
/// Network adapters insert this extension from the accepted connection, never from request
/// headers. Middleware can use it as the first input to an explicit proxy-trust policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionInfo {
    peer_addr: SocketAddr,
}

impl ConnectionInfo {
    /// Creates connection metadata from an adapter-observed peer address.
    #[must_use]
    pub const fn new(peer_addr: SocketAddr) -> Self {
        Self { peer_addr }
    }

    /// Returns the directly connected peer address.
    #[must_use]
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

/// The configured template of a route selected by the application router.
///
/// Network adapters must not construct this from a request URI. The router derives it from its
/// configured route table, so observability can use it without recording a user-controlled path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RouteTemplate(String);

impl RouteTemplate {
    /// Creates route metadata from a router-configured template.
    #[must_use]
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    /// Returns the configured route template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A low-cardinality router outcome for response-layer observability.
///
/// The special values are framework-reserved and do not contain a request URI. A matched route
/// holds the router-configured [`RouteTemplate`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RouteClassification {
    /// A route selected for the request method and path.
    Matched(RouteTemplate),
    /// An application-defined fallback handled the request.
    Fallback,
    /// The path matched a route but not the request method.
    MethodNotAllowed,
    /// Neither a route nor an application fallback handled the path.
    NotFound,
}

impl RouteClassification {
    /// Builds the classification for a matched configured route.
    #[must_use]
    pub const fn matched(template: RouteTemplate) -> Self {
        Self::Matched(template)
    }

    /// Builds the classification for an application fallback.
    #[must_use]
    pub const fn fallback() -> Self {
        Self::Fallback
    }

    /// Builds the classification for a method mismatch.
    #[must_use]
    pub const fn method_not_allowed() -> Self {
        Self::MethodNotAllowed
    }

    /// Builds the classification for an unmatched path.
    #[must_use]
    pub const fn not_found() -> Self {
        Self::NotFound
    }

    /// Returns the configured template or a framework-reserved outcome label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Matched(template) => template.as_str(),
            Self::Fallback => "<fallback>",
            Self::MethodNotAllowed => "<method-not-allowed>",
            Self::NotFound => "<not-found>",
        }
    }
}

/// Implement this trait to extract a strongly typed request header.
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

/// Matched route parameters, kept separate from query and request state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteParams(Vec<(String, String)>);

impl RouteParams {
    /// Builds parameters from a matched route.
    #[must_use]
    pub fn new(params: Vec<(String, String)>) -> Self {
        Self(params)
    }

    /// Looks up a named parameter.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }

    /// Returns the matched parameters in route order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

/// Cloneable type-indexed application state used by [`State`].
#[derive(Clone, Default)]
pub struct StateStore {
    values: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateStore")
            .field("registered_types", &self.values.len())
            .finish()
    }
}

impl StateStore {
    /// Adds or replaces a state value by concrete type.
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.values).insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Returns a clone of a typed state handle.
    #[must_use]
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
    }
}

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
            let content_type = request
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            if !is_json_content_type(content_type) {
                return Err(Error::unsupported_media_type(
                    "expected an application/json content type",
                ));
            }

            let bytes = collect_body(request).await?;
            serde_json::from_slice(&bytes)
                .map(Self)
                .map_err(|error| Error::bad_request(format!("invalid JSON body: {error}")))
        })
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
                .map_err(|error| Error::bad_request(format!("invalid query string: {error}")))
        })
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
                serializer.append_pair(key, value);
            }
            serde_urlencoded::from_str(&serializer.finish())
                .map(Self)
                .map_err(|error| Error::bad_request(format!("invalid path parameters: {error}")))
        })
    }
}

impl<T> FromRequest for State<T>
where
    T: Send + Sync + 'static,
{
    fn from_request<'a>(
        _request: &'a mut Request,
        _params: &'a RouteParams,
        state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { state.get::<T>().map(Self).ok_or_else(Error::internal) })
    }
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
            let value = request
                .headers()
                .get(name)
                .ok_or_else(|| Error::bad_request(format!("missing {} header", T::NAME)))?;
            T::from_header(value).map(Self)
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
#[must_use]
pub fn response(status: StatusCode, body: Body) -> Response {
    let mut response = HttpResponse::new(body);
    *response.status_mut() = status;
    response
}

/// Encodes a value as a JSON response.
///
/// # Errors
///
/// Returns an internal error when `value` cannot be serialized as JSON.
pub fn json_response<T>(status: StatusCode, value: &T) -> Result<Response>
where
    T: Serialize,
{
    let encoded = serde_json::to_vec(value).map_err(|_| Error::internal())?;
    let mut response = response(status, full_body(encoded));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    Ok(response)
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

fn text_response(status: StatusCode, value: impl Into<Bytes>) -> Response {
    let mut response = response(status, full_body(value));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn is_json_content_type(content_type: &str) -> bool {
    let mime = content_type.split(';').next().unwrap_or_default().trim();
    mime.eq_ignore_ascii_case("application/json") || mime.ends_with("+json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_response_is_json_and_sanitized() {
        let response = Error::bad_request("invalid input").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
    }

    #[test]
    fn state_store_is_type_indexed() {
        let mut store = StateStore::default();
        store.insert(String::from("configured"));
        assert_eq!(
            store.get::<String>().as_deref(),
            Some(&String::from("configured"))
        );
        assert!(store.get::<u64>().is_none());
    }
}
