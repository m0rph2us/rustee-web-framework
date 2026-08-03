//! Middleware primitives that preserve Tower's `Layer` and `Service` contracts.

use std::{
    convert::Infallible,
    io,
    net::IpAddr,
    panic::AssertUnwindSafe,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_compression::tokio::bufread::{BrotliEncoder, GzipEncoder};
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode,
    header::{
        ACCEPT_ENCODING, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD,
        CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, FORWARDED,
        ORIGIN, RANGE, VARY,
    },
};
use http_body::Frame;
use http_body_util::{BodyExt, BodyStream, StreamBody};
use rustee_core::{
    Body, BoxError, ConnectionInfo, Error, FromRequest, IntoResponse, Request, Response,
    RouteParams, StateStore, empty_body, response,
};
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};
use tower::{Layer, Service, util::BoxCloneService};

pub use tower::ServiceBuilder;

const MAX_FORWARDED_CHAIN_HOPS: usize = 16;
const MAX_FORWARDED_HEADER_BYTES: usize = 2_048;
const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_FORWARDED_HOST: &str = "x-forwarded-host";
const X_FORWARDED_PROTO: &str = "x-forwarded-proto";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentCoding {
    Brotli,
    Gzip,
}

impl ContentCoding {
    const fn token(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Gzip => "gzip",
        }
    }

    const fn header_value(self) -> HeaderValue {
        match self {
            Self::Brotli => HeaderValue::from_static("br"),
            Self::Gzip => HeaderValue::from_static("gzip"),
        }
    }
}

/// Streaming response compression with explicit gzip and Brotli negotiation.
///
/// The layer compresses textual and structured response media types only. Existing content
/// codings, `Cache-Control: no-transform`, range requests, `HEAD` requests, and bodyless
/// responses pass through unchanged. Applied responses gain `Vary: Accept-Encoding`; their
/// original `Content-Length` is removed because the compressed stream has a different length.
#[derive(Clone, Copy, Debug)]
#[must_use = "a compression layer must be layered onto an application to have an effect"]
pub struct CompressionLayer {
    brotli: bool,
    gzip: bool,
}

impl CompressionLayer {
    /// Creates a layer that negotiates Brotli before gzip when their qualities are equal.
    pub const fn new() -> Self {
        Self {
            brotli: true,
            gzip: true,
        }
    }

    /// Enables or disables Brotli negotiation.
    pub const fn with_brotli(mut self, enabled: bool) -> Self {
        self.brotli = enabled;
        self
    }

    /// Enables or disables gzip negotiation.
    pub const fn with_gzip(mut self, enabled: bool) -> Self {
        self.gzip = enabled;
        self
    }

    fn select_coding(self, headers: &HeaderMap) -> Option<ContentCoding> {
        let brotli = self
            .brotli
            .then(|| quality_for(headers, ContentCoding::Brotli));
        let gzip = self.gzip.then(|| quality_for(headers, ContentCoding::Gzip));

        match (brotli.flatten(), gzip.flatten()) {
            (Some(brotli), Some(gzip)) if brotli >= gzip => Some(ContentCoding::Brotli),
            (Some(_) | None, Some(_)) => Some(ContentCoding::Gzip),
            (Some(_), None) => Some(ContentCoding::Brotli),
            (None, None) => None,
        }
    }
}

impl Default for CompressionLayer {
    fn default() -> Self {
        Self::new()
    }
}

/// Opt-in HTTP boundary that turns an uncommitted handler panic into a redacted 500 response.
///
/// This layer catches only a panic while its inner service is producing a [`Response`]. It cannot
/// replace the process-wide panic hook, recover a `panic = "abort"` build, or change a panic from
/// a body stream after response headers have been committed. Place it outside the handlers and
/// middleware whose call futures need the same response boundary.
#[derive(Clone, Copy, Debug, Default)]
#[must_use = "a panic-catch layer must be layered onto an application to have an effect"]
pub struct PanicCatchLayer;

impl PanicCatchLayer {
    /// Creates a panic boundary with Rustee's fixed public internal-error response.
    pub const fn new() -> Self {
        Self
    }
}

/// Service produced by [`PanicCatchLayer`].
#[derive(Clone, Debug)]
pub struct PanicCatch {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for PanicCatchLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = PanicCatch;

    fn layer(&self, inner: S) -> Self::Service {
        PanicCatch {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for PanicCatch {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            match AssertUnwindSafe(inner.call(request)).catch_unwind().await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(never)) => match never {},
                Err(_) => {
                    tracing::error!("Rustee service panicked before producing an HTTP response");
                    Ok(Error::internal().into_response())
                }
            }
        })
    }
}

/// Service produced by [`CompressionLayer`].
#[derive(Clone, Debug)]
pub struct Compression {
    inner: BoxCloneService<Request, Response, Infallible>,
    layer: CompressionLayer,
}

impl<S> Layer<S> for CompressionLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = Compression;

    fn layer(&self, inner: S) -> Self::Service {
        Compression {
            inner: BoxCloneService::new(inner),
            layer: *self,
        }
    }
}

impl Service<Request> for Compression {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        let may_transform =
            request.method() != Method::HEAD && !request.headers().contains_key(RANGE);
        let coding = if may_transform {
            self.layer.select_coding(request.headers())
        } else {
            None
        };

        Box::pin(async move {
            let response = inner.call(request).await?;
            Ok(if may_transform && is_compressible_response(&response) {
                match coding {
                    Some(coding) => compress_response(response, coding),
                    None => vary_identity_response(response),
                }
            } else {
                response
            })
        })
    }
}

fn compress_response(response: Response, coding: ContentCoding) -> Response {
    let (mut parts, body) = response.into_parts();
    parts.headers.remove(CONTENT_LENGTH);
    parts
        .headers
        .insert(CONTENT_ENCODING, coding.header_value());
    add_vary_accept_encoding(&mut parts.headers);
    Response::from_parts(parts, compressed_body(body, coding))
}

fn vary_identity_response(mut response: Response) -> Response {
    add_vary_accept_encoding(response.headers_mut());
    response
}

fn is_compressible_response(response: &Response) -> bool {
    let status = response.status();
    if status.is_informational()
        || matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED)
        || response.headers().contains_key(CONTENT_ENCODING)
        || response.headers().contains_key(CONTENT_RANGE)
        || cache_control_no_transform(response.headers())
    {
        return false;
    }

    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "0")
    {
        return false;
    }

    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_compressible_content_type)
}

fn cache_control_no_transform(headers: &HeaderMap) -> bool {
    headers.get_all(CACHE_CONTROL).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|directive| directive.eq_ignore_ascii_case("no-transform"))
        })
    })
}

fn is_compressible_content_type(value: &str) -> bool {
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type.starts_with("text/")
        || matches!(
            media_type.as_str(),
            "application/javascript"
                | "application/json"
                | "application/ld+json"
                | "application/manifest+json"
                | "application/xhtml+xml"
                | "application/xml"
                | "application/rss+xml"
                | "application/atom+xml"
                | "image/svg+xml"
        )
        || media_type.ends_with("+json")
        || media_type.ends_with("+xml")
}

fn quality_for(headers: &HeaderMap, coding: ContentCoding) -> Option<u16> {
    let mut explicit = None;
    let mut wildcard = None;
    let mut found = false;

    for value in &headers.get_all(ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        found = true;
        for item in value.split(',') {
            let mut parameters = item.split(';');
            let token = parameters.next().unwrap_or_default().trim();
            let quality = parameters
                .find_map(|parameter| {
                    let (name, value) = parameter.trim().split_once('=')?;
                    name.trim()
                        .eq_ignore_ascii_case("q")
                        .then_some(value.trim())
                })
                .map_or(Some(1_000), parse_quality)
                .unwrap_or(0);

            if token.eq_ignore_ascii_case(coding.token()) {
                explicit = Some(explicit.unwrap_or(0).max(quality));
            } else if token == "*" {
                wildcard = Some(wildcard.unwrap_or(0).max(quality));
            }
        }
    }

    if !found {
        return None;
    }
    explicit.or(wildcard).filter(|quality| *quality > 0)
}

fn parse_quality(value: &str) -> Option<u16> {
    if value == "0" || value == "0." {
        return Some(0);
    }
    if value == "1" || value == "1." {
        return Some(1_000);
    }
    let (whole, fraction) = value.split_once('.')?;
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => {
            let parsed = fraction.parse::<u16>().ok()?;
            let scale = match fraction.len() {
                1 => 100,
                2 => 10,
                3 => 1,
                _ => return None,
            };
            Some(parsed * scale)
        }
        "1" if fraction.bytes().all(|byte| byte == b'0') => Some(1_000),
        _ => None,
    }
}

fn add_vary_accept_encoding(headers: &mut HeaderMap) {
    let already_varies = headers.get_all(VARY).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|field| field == "*" || field.eq_ignore_ascii_case(ACCEPT_ENCODING.as_str()))
        })
    });
    if !already_varies {
        headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
    }
}

fn compressed_body(body: Body, coding: ContentCoding) -> Body {
    let trailers = Arc::new(Mutex::<Option<HeaderMap>>::new(None));
    let input_trailers = Arc::clone(&trailers);
    let input = BodyStream::new(body).filter_map(move |frame| {
        let trailers = Arc::clone(&input_trailers);
        async move {
            match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(data) => Some(Ok(data)),
                    Err(frame) => match frame.into_trailers() {
                        Ok(values) => match trailers.lock() {
                            Ok(mut stored) => {
                                if let Some(existing) = stored.as_mut() {
                                    existing.extend(values);
                                } else {
                                    *stored = Some(values);
                                }
                                None
                            }
                            Err(_) => {
                                Some(Err(io::Error::other("response trailers lock poisoned")))
                            }
                        },
                        Err(_) => Some(Err(io::Error::other(
                            "response frame is neither data nor trailers",
                        ))),
                    },
                },
                Err(error) => Some(Err(io::Error::other(error))),
            }
        }
    });
    let reader = StreamReader::new(input);

    match coding {
        ContentCoding::Brotli => compressed_reader_body(BrotliEncoder::new(reader), trailers),
        ContentCoding::Gzip => compressed_reader_body(GzipEncoder::new(reader), trailers),
    }
}

fn compressed_reader_body<R>(reader: R, trailers: Arc<Mutex<Option<HeaderMap>>>) -> Body
where
    R: AsyncRead + Send + 'static,
{
    let stream = async_stream::stream! {
        let stream = ReaderStream::with_capacity(reader, 16 * 1024);
        futures_util::pin_mut!(stream);
        while let Some(chunk) = stream.next().await {
            yield chunk
                .map(Frame::data)
                .map_err(|error| -> BoxError { Box::new(error) });
        }
        match take_trailers(&trailers) {
            Ok(Some(values)) => yield Ok(Frame::trailers(values)),
            Ok(None) => {}
            Err(error) => yield Err(error),
        }
    };
    BodyExt::boxed_unsync(StreamBody::new(stream))
}

fn take_trailers(trailers: &Mutex<Option<HeaderMap>>) -> Result<Option<HeaderMap>, BoxError> {
    let mut stored = trailers.lock().map_err(|_| -> BoxError {
        Box::new(io::Error::other("response trailers lock poisoned"))
    })?;
    Ok(stored.take())
}

/// One IP network trusted to terminate or forward traffic to this application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedProxyNetwork {
    network: IpAddr,
    prefix_length: u8,
}

impl TrustedProxyNetwork {
    /// Creates a normalized IPv4 or IPv6 CIDR network.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::InvalidPrefixLength`] when the prefix does not fit the IP
    /// family. A `/0` network is deliberately rejected because it would trust every peer.
    pub fn new(network: IpAddr, prefix_length: u8) -> Result<Self, TrustedProxyError> {
        let width = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_length == 0 || prefix_length > width {
            return Err(TrustedProxyError::InvalidPrefixLength);
        }
        Ok(Self {
            network: normalize_ip(network, prefix_length),
            prefix_length,
        })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let shift = 32 - u32::from(self.prefix_length);
                u32::from(network) >> shift == u32::from(address) >> shift
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let shift = 128 - u32::from(self.prefix_length);
                u128::from(network) >> shift == u128::from(address) >> shift
            }
            _ => false,
        }
    }
}

/// Explicit policy for one or more trusted reverse-proxy networks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedProxyPolicy {
    networks: Vec<TrustedProxyNetwork>,
    forwarded_chain_hops: usize,
}

impl TrustedProxyPolicy {
    /// Creates a policy that trusts the supplied non-empty network allowlist.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::EmptyNetworkAllowlist`] when no proxy networks are supplied.
    pub fn new(
        networks: impl IntoIterator<Item = TrustedProxyNetwork>,
    ) -> Result<Self, TrustedProxyError> {
        let networks = networks.into_iter().collect::<Vec<_>>();
        if networks.is_empty() {
            return Err(TrustedProxyError::EmptyNetworkAllowlist);
        }
        Ok(Self {
            networks,
            forwarded_chain_hops: 0,
        })
    }

    /// Allows up to `hops` trusted intermediary `Forwarded` elements before the client address.
    ///
    /// The direct transport peer must still match this policy. The default is zero, preserving the
    /// one-element single-hop contract. Earlier chain elements cannot supply `proto` or `host`;
    /// those values must be asserted by the direct trusted proxy in the rightmost element.
    ///
    /// # Errors
    ///
    /// Returns [`TrustedProxyError::InvalidForwardedChainHops`] when `hops` is zero or exceeds the
    /// bounded parser limit.
    pub fn with_forwarded_chain_hops(mut self, hops: usize) -> Result<Self, TrustedProxyError> {
        if hops == 0 || hops > MAX_FORWARDED_CHAIN_HOPS {
            return Err(TrustedProxyError::InvalidForwardedChainHops);
        }
        self.forwarded_chain_hops = hops;
        Ok(self)
    }

    fn trusts(&self, address: IpAddr) -> bool {
        self.networks
            .iter()
            .copied()
            .any(|network| network.contains(address))
    }
}

/// Invalid trusted-proxy configuration or forwarded-header input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TrustedProxyError {
    /// A CIDR prefix was zero or exceeded its address family width.
    #[error("trusted proxy network prefix must be within its IP family and not /0")]
    InvalidPrefixLength,
    /// No trusted networks were configured.
    #[error("trusted proxy policy requires at least one network")]
    EmptyNetworkAllowlist,
    /// The configured forwarded chain depth was zero or exceeded the bounded parser limit.
    #[error("trusted proxy forwarded chain hops must be between 1 and 16")]
    InvalidForwardedChainHops,
}

/// Forwarded client data verified through an explicitly trusted reverse-proxy chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardedContext {
    client_ip: IpAddr,
    scheme: Option<String>,
    host: Option<String>,
}

impl ForwardedContext {
    /// Returns the client IP asserted by the trusted direct proxy.
    #[must_use]
    pub const fn client_ip(&self) -> IpAddr {
        self.client_ip
    }
    /// Returns the trusted external scheme when the proxy supplied `proto`.
    #[must_use]
    pub fn scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }
    /// Returns the trusted external host when the proxy supplied `host`.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
}

impl FromRequest for ForwardedContext {
    fn from_request<'a>(
        request: &'a mut Request,
        _: &'a RouteParams,
        _: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forwarded_context_missing",
                    "trusted proxy context is required",
                )
            })
        })
    }
}

/// Layer that normalizes one selected forwarded-header family only when the direct peer is trusted.
#[derive(Clone, Debug)]
#[must_use = "a trusted proxy policy must be layered onto a service to have an effect"]
pub struct TrustedProxyLayer {
    policy: TrustedProxyPolicy,
    header_family: TrustedProxyHeaderFamily,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrustedProxyHeaderFamily {
    Forwarded,
    XForwarded,
}

impl TrustedProxyLayer {
    pub const fn new(policy: TrustedProxyPolicy) -> Self {
        Self {
            policy,
            header_family: TrustedProxyHeaderFamily::Forwarded,
        }
    }

    /// Uses the `X-Forwarded-For`/`Proto`/`Host` family instead of RFC 7239 `Forwarded`.
    ///
    /// This is explicit because deployments must configure their direct trusted proxy to sanitize
    /// exactly one forwarded-header family. The same policy chain-hop bound applies.
    pub const fn with_x_forwarded(mut self) -> Self {
        self.header_family = TrustedProxyHeaderFamily::XForwarded;
        self
    }
}
#[derive(Clone, Debug)]
pub struct TrustedProxy {
    inner: BoxCloneService<Request, Response, Infallible>,
    policy: TrustedProxyPolicy,
    header_family: TrustedProxyHeaderFamily,
}
impl<S> Layer<S> for TrustedProxyLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = TrustedProxy;
    fn layer(&self, inner: S) -> Self::Service {
        TrustedProxy {
            inner: BoxCloneService::new(inner),
            policy: self.policy.clone(),
            header_family: self.header_family,
        }
    }
}
impl Service<Request> for TrustedProxy {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;
    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }
    fn call(&mut self, mut request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        let policy = self.policy.clone();
        let header_family = self.header_family;
        Box::pin(async move {
            let peer = request
                .extensions()
                .get::<ConnectionInfo>()
                .copied()
                .ok_or_else(|| {
                    Error::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "connection_info_missing",
                        "transport connection metadata is required",
                    )
                    .into_response()
                });
            let peer = match peer {
                Ok(peer) => peer,
                Err(response) => return Ok(response),
            };
            if policy.trusts(peer.peer_addr().ip()) {
                let context = match header_family {
                    TrustedProxyHeaderFamily::Forwarded => {
                        parse_forwarded_headers(request.headers(), &policy)
                    }
                    TrustedProxyHeaderFamily::XForwarded => {
                        parse_x_forwarded_headers(request.headers(), &policy)
                    }
                };
                match context {
                    Ok(Some(context)) => {
                        request.extensions_mut().insert(context);
                    }
                    Ok(None) => {}
                    Err(()) => {
                        return Ok(Error::new(
                            StatusCode::BAD_REQUEST,
                            "invalid_forwarded_header",
                            "the trusted proxy forwarded header is invalid",
                        )
                        .into_response());
                    }
                }
            }
            inner.call(request).await
        })
    }
}

fn normalize_ip(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let shift = 32 - u32::from(prefix);
            IpAddr::V4((u32::from(address) >> shift << shift).into())
        }
        IpAddr::V6(address) => {
            let shift = 128 - u32::from(prefix);
            IpAddr::V6((u128::from(address) >> shift << shift).into())
        }
    }
}
#[derive(Debug)]
struct ForwardedElement {
    client_ip: IpAddr,
    scheme: Option<String>,
    host: Option<String>,
}

fn parse_forwarded_headers(
    headers: &HeaderMap,
    policy: &TrustedProxyPolicy,
) -> Result<Option<ForwardedContext>, ()> {
    let value = single_header(headers, FORWARDED)?;
    value
        .map(|value| parse_forwarded(value, policy))
        .transpose()
}

fn parse_forwarded(
    value: &HeaderValue,
    policy: &TrustedProxyPolicy,
) -> Result<ForwardedContext, ()> {
    let value = value.to_str().map_err(|_| ())?;
    if value.len() > MAX_FORWARDED_HEADER_BYTES {
        return Err(());
    }
    let elements = value
        .split(',')
        .map(|element| parse_forwarded_element(element.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    if elements.is_empty() || elements.len() > policy.forwarded_chain_hops + 1 {
        return Err(());
    }
    if elements[..elements.len() - 1]
        .iter()
        .any(|element| element.scheme.is_some() || element.host.is_some())
    {
        return Err(());
    }
    let edge = elements.last().ok_or(())?;
    let mut trusted_hops = 0;
    for element in elements.iter().rev() {
        if trusted_hops < policy.forwarded_chain_hops && policy.trusts(element.client_ip) {
            trusted_hops += 1;
            continue;
        }
        return Ok(ForwardedContext {
            client_ip: element.client_ip,
            scheme: edge.scheme.clone(),
            host: edge.host.clone(),
        });
    }
    Err(())
}

fn parse_x_forwarded_headers(
    headers: &HeaderMap,
    policy: &TrustedProxyPolicy,
) -> Result<Option<ForwardedContext>, ()> {
    let client = single_header(headers, X_FORWARDED_FOR)?;
    let scheme = single_header(headers, X_FORWARDED_PROTO)?;
    let host = single_header(headers, X_FORWARDED_HOST)?;
    let Some(client) = client else {
        return (scheme.is_none() && host.is_none())
            .then_some(None)
            .ok_or(());
    };
    let client = client.to_str().map_err(|_| ())?;
    if client.len() > MAX_FORWARDED_HEADER_BYTES {
        return Err(());
    }
    let clients = client
        .split(',')
        .map(str::trim)
        .map(|value| IpAddr::from_str(value).map_err(|_| ()))
        .collect::<Result<Vec<_>, _>>()?;
    if clients.is_empty() || clients.len() > policy.forwarded_chain_hops + 1 {
        return Err(());
    }
    let client_ip = select_client_ip(&clients, policy)?;
    let scheme = scheme.map(parse_x_forwarded_scheme).transpose()?;
    let host = host.map(parse_x_forwarded_host).transpose()?;
    Ok(Some(ForwardedContext {
        client_ip,
        scheme,
        host,
    }))
}

fn single_header(
    headers: &HeaderMap,
    name: impl http::header::AsHeaderName,
) -> Result<Option<&HeaderValue>, ()> {
    let values = headers.get_all(name);
    let mut values = values.iter();
    let first = values.next();
    values.next().is_none().then_some(first).ok_or(())
}

fn select_client_ip(clients: &[IpAddr], policy: &TrustedProxyPolicy) -> Result<IpAddr, ()> {
    let mut trusted_hops = 0;
    for client in clients.iter().rev() {
        if trusted_hops < policy.forwarded_chain_hops && policy.trusts(*client) {
            trusted_hops += 1;
            continue;
        }
        return Ok(*client);
    }
    Err(())
}

fn parse_x_forwarded_scheme(value: &HeaderValue) -> Result<String, ()> {
    let value = value.to_str().map_err(|_| ())?;
    matches!(value, "http" | "https")
        .then(|| value.to_owned())
        .ok_or(())
}

fn parse_x_forwarded_host(value: &HeaderValue) -> Result<String, ()> {
    let value = value.to_str().map_err(|_| ())?;
    (!value.contains([',', ' ', '"', '@']) && value.parse::<http::uri::Authority>().is_ok())
        .then(|| value.to_owned())
        .ok_or(())
}

fn parse_forwarded_element(value: &str) -> Result<ForwardedElement, ()> {
    let mut client_ip = None;
    let mut scheme = None;
    let mut host = None;
    for item in value.split(';') {
        let (name, value) = item.trim().split_once('=').ok_or(())?;
        if value.is_empty() || value.contains([' ', '\"']) {
            return Err(());
        }
        match name.to_ascii_lowercase().as_str() {
            "for" if client_ip.is_none() => {
                client_ip = Some(IpAddr::from_str(value).map_err(|_| ())?);
            }
            "proto" if scheme.is_none() && matches!(value, "http" | "https") => {
                scheme = Some(value.to_owned());
            }
            "host"
                if host.is_none()
                    && !value.contains('@')
                    && value.parse::<http::uri::Authority>().is_ok() =>
            {
                host = Some(value.to_owned());
            }
            _ => return Err(()),
        }
    }
    Ok(ForwardedElement {
        client_ip: client_ip.ok_or(())?,
        scheme,
        host,
    })
}

/// A conservative CORS layer for a single explicit allowed origin.
#[derive(Clone, Debug)]
#[must_use = "a CORS builder must be layered onto an application to have an effect"]
pub struct CorsLayer {
    allowed_origin: HeaderValue,
    allowed_methods: HeaderValue,
    allowed_headers: HeaderValue,
    allow_credentials: bool,
}

impl CorsLayer {
    /// Creates a CORS policy for one explicit origin.
    pub fn new(allowed_origin: HeaderValue) -> Self {
        Self {
            allowed_origin,
            allowed_methods: HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE, OPTIONS"),
            allowed_headers: HeaderValue::from_static("content-type, authorization"),
            allow_credentials: false,
        }
    }

    /// Sets the allowed request methods advertised to browsers.
    pub fn allow_methods(mut self, methods: HeaderValue) -> Self {
        self.allowed_methods = methods;
        self
    }

    /// Sets the allowed request headers advertised to browsers.
    pub fn allow_headers(mut self, headers: HeaderValue) -> Self {
        self.allowed_headers = headers;
        self
    }

    /// Allows credentials only for the explicitly configured origin.
    pub fn allow_credentials(mut self, allow_credentials: bool) -> Self {
        self.allow_credentials = allow_credentials;
        self
    }

    fn apply(&self, response: &mut Response) {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allowed_origin.clone());
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_METHODS, self.allowed_methods.clone());
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_HEADERS, self.allowed_headers.clone());
        if self.allow_credentials {
            response.headers_mut().insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
    }
}

/// Service produced by [`CorsLayer`].
#[derive(Clone, Debug)]
pub struct Cors {
    inner: BoxCloneService<Request, Response, Infallible>,
    policy: CorsLayer,
}

impl<S> Layer<S> for CorsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = Cors;

    fn layer(&self, inner: S) -> Self::Service {
        Cors {
            inner: BoxCloneService::new(inner),
            policy: self.clone(),
        }
    }
}

impl Service<Request> for Cors {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if is_preflight(&request) {
            let policy = self.policy.clone();
            return Box::pin(async move {
                let mut response = response(StatusCode::NO_CONTENT, empty_body());
                policy.apply(&mut response);
                Ok(response)
            });
        }

        let mut inner = self.inner.clone();
        let policy = self.policy.clone();
        Box::pin(async move {
            let mut response = Service::call(&mut inner, request).await?;
            policy.apply(&mut response);
            Ok(response)
        })
    }
}

fn is_preflight(request: &Request) -> bool {
    request.method() == Method::OPTIONS
        && request.headers().contains_key(ORIGIN)
        && request
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        io::Cursor,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        str::FromStr,
    };

    use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder};
    use bytes::Bytes;
    use http::{
        HeaderMap, HeaderValue, Method, Request as HttpRequest, StatusCode,
        header::{
            ACCEPT_ENCODING, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD,
            CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, FORWARDED, ORIGIN,
            RANGE, VARY,
        },
    };
    use http_body::Frame;
    use http_body_util::{BodyExt, StreamBody};
    use rustee_core::{BoxError, ConnectionInfo, IntoResponse, empty_body, response};
    use tokio::io::{AsyncReadExt, BufReader};
    use tower::{Layer, ServiceExt};

    use super::{
        CompressionLayer, CorsLayer, ForwardedContext, MAX_FORWARDED_CHAIN_HOPS, PanicCatchLayer,
        TrustedProxyError, TrustedProxyLayer, TrustedProxyNetwork, TrustedProxyPolicy,
        X_FORWARDED_FOR, X_FORWARDED_HOST, X_FORWARDED_PROTO,
    };
    use rustee_router::App;

    const DOCUMENT: &str = "A useful Rustee document that compresses cleanly.";

    async fn panic_handler() -> &'static str {
        panic!("private panic detail must not reach an HTTP response");
    }

    #[tokio::test]
    async fn panic_catch_layer_returns_a_redacted_internal_response() {
        let service = PanicCatchLayer::new().layer(App::new().get("/panic", panic_handler));
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/panic")
            .body(empty_body())
            .unwrap();

        let response = service.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body,
            r#"{"error":{"code":"internal_error","message":"an internal server error occurred"}}"#
        );
        assert!(!String::from_utf8_lossy(&body).contains("private panic detail"));
    }

    #[tokio::test]
    async fn cors_preflight_does_not_reach_the_application() {
        let service = CorsLayer::new("https://app.example.test".parse().unwrap())
            .layer(App::new().get("/resource", || async { "unexpected" }));
        let request = HttpRequest::builder()
            .method("OPTIONS")
            .uri("/resource")
            .header(ORIGIN, "https://app.example.test")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .body(empty_body())
            .unwrap();

        let response = service.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 204);
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example.test"
        );
    }

    #[tokio::test]
    async fn compression_negotiates_brotli_and_updates_vary() {
        let service = CompressionLayer::new().layer(App::new().get("/document", || async {
            let mut response = DOCUMENT.into_response();
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_static("47"));
            response
                .headers_mut()
                .append(VARY, HeaderValue::from_static("Origin"));
            response
        }));

        let response = service
            .oneshot(compression_request("gzip;q=0.4, br"))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "br");
        assert!(response.headers().get(CONTENT_LENGTH).is_none());
        assert_eq!(
            response
                .headers()
                .get_all(VARY)
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["Origin", "Accept-Encoding"]
        );

        assert_eq!(
            decode_brotli(response.into_body().collect().await.unwrap().to_bytes()).await,
            DOCUMENT.as_bytes()
        );
    }

    #[tokio::test]
    async fn compression_honors_gzip_quality_and_existing_coding() {
        let service =
            CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
        let response = service
            .clone()
            .oneshot(compression_request("br;q=0.5, gzip;q=0.8"))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
        assert_eq!(
            decode_gzip(response.into_body().collect().await.unwrap().to_bytes()).await,
            DOCUMENT.as_bytes()
        );

        let existing = CompressionLayer::new().layer(App::new().get("/document", || async {
            let mut response = DOCUMENT.into_response();
            response
                .headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
            response
        }));
        let response = existing.oneshot(compression_request("gzip")).await.unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "identity");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            DOCUMENT
        );
    }

    #[tokio::test]
    async fn compression_marks_an_identity_representation_as_varying() {
        let service =
            CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
        let response = service
            .oneshot(compression_request("identity"))
            .await
            .unwrap();

        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert_eq!(response.headers()[VARY], "Accept-Encoding");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            DOCUMENT
        );
    }

    #[tokio::test]
    async fn compression_bypasses_range_head_and_no_transform_responses() {
        let service =
            CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
        let mut range = compression_request("gzip");
        range
            .headers_mut()
            .insert(RANGE, HeaderValue::from_static("bytes=0-9"));
        let response = service.clone().oneshot(range).await.unwrap();
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            DOCUMENT
        );

        let head = CompressionLayer::new().layer(tower::service_fn(|_| async {
            Ok::<_, Infallible>(DOCUMENT.into_response())
        }));
        let mut request = compression_request("gzip");
        *request.method_mut() = Method::HEAD;
        let response = head.oneshot(request).await.unwrap();
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            DOCUMENT
        );

        let no_transform = CompressionLayer::new().layer(App::new().get("/document", || async {
            let mut response = DOCUMENT.into_response();
            response.headers_mut().insert(
                CACHE_CONTROL,
                HeaderValue::from_static("public, no-transform"),
            );
            response
        }));
        let response = no_transform
            .oneshot(compression_request("gzip"))
            .await
            .unwrap();
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
    }

    #[tokio::test]
    async fn compression_preserves_response_trailers() {
        let service = CompressionLayer::new().layer(tower::service_fn(|_| async {
            let mut trailers = HeaderMap::new();
            trailers.insert("x-checksum", HeaderValue::from_static("verified"));
            let source = async_stream::stream! {
                yield Ok::<_, BoxError>(Frame::data(Bytes::from_static(DOCUMENT.as_bytes())));
                yield Ok(Frame::trailers(trailers));
            };
            let body = BodyExt::boxed_unsync(StreamBody::new(source));
            let mut response = response(StatusCode::OK, body);
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            Ok::<_, Infallible>(response)
        }));

        let response = service.oneshot(compression_request("gzip")).await.unwrap();
        let collected = response.into_body().collect().await.unwrap();
        assert_eq!(
            collected.trailers().unwrap()["x-checksum"],
            HeaderValue::from_static("verified")
        );
        assert_eq!(decode_gzip(collected.to_bytes()).await, DOCUMENT.as_bytes());
    }

    fn compression_request(accept_encoding: &str) -> rustee_core::Request {
        HttpRequest::builder()
            .method(Method::GET)
            .uri("/document")
            .header(ACCEPT_ENCODING, accept_encoding)
            .body(empty_body())
            .unwrap()
    }

    async fn decode_gzip(body: Bytes) -> Vec<u8> {
        let mut decoder = GzipDecoder::new(BufReader::new(Cursor::new(body)));
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).await.unwrap();
        output
    }

    async fn decode_brotli(body: Bytes) -> Vec<u8> {
        let mut decoder = BrotliDecoder::new(BufReader::new(Cursor::new(body)));
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).await.unwrap();
        output
    }

    fn request(peer: IpAddr, forwarded: Option<&str>) -> rustee_core::Request {
        let mut builder = HttpRequest::builder().method("GET").uri("/context");
        if let Some(forwarded) = forwarded {
            builder = builder.header(FORWARDED, forwarded);
        }
        let mut request = builder.body(empty_body()).unwrap();
        request
            .extensions_mut()
            .insert(ConnectionInfo::new(SocketAddr::new(peer, 443)));
        request
    }

    fn x_forwarded_request(
        peer: IpAddr,
        client: Option<&str>,
        scheme: Option<&str>,
        host: Option<&str>,
    ) -> rustee_core::Request {
        let mut builder = HttpRequest::builder().method("GET").uri("/context");
        if let Some(client) = client {
            builder = builder.header(X_FORWARDED_FOR, client);
        }
        if let Some(scheme) = scheme {
            builder = builder.header(X_FORWARDED_PROTO, scheme);
        }
        if let Some(host) = host {
            builder = builder.header(X_FORWARDED_HOST, host);
        }
        let mut request = builder.body(empty_body()).unwrap();
        request
            .extensions_mut()
            .insert(ConnectionInfo::new(SocketAddr::new(peer, 443)));
        request
    }

    fn trusted_policy() -> TrustedProxyPolicy {
        TrustedProxyPolicy::new([TrustedProxyNetwork::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
            8,
        )
        .unwrap()])
        .unwrap()
    }

    fn multi_hop_policy() -> TrustedProxyPolicy {
        TrustedProxyPolicy::new([
            TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8).unwrap(),
            TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 0)), 24).unwrap(),
        ])
        .unwrap()
        .with_forwarded_chain_hops(1)
        .unwrap()
    }

    #[tokio::test]
    async fn trusted_proxy_normalizes_one_forwarded_hop() {
        let service = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
            "/context",
            |context: ForwardedContext| async move {
                format!(
                    "{}:{}:{}",
                    context.client_ip(),
                    context.scheme().unwrap(),
                    context.host().unwrap()
                )
            },
        ));
        let response = service
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=203.0.113.10;proto=https;host=app.example.test"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn trusted_proxy_can_select_a_client_behind_one_trusted_intermediary() {
        let service = TrustedProxyLayer::new(multi_hop_policy()).layer(App::new().get(
            "/context",
            |context: ForwardedContext| async move {
                format!(
                    "{}:{}:{}",
                    context.client_ip(),
                    context.scheme().unwrap(),
                    context.host().unwrap()
                )
            },
        ));
        let response = service
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=203.0.113.10, for=192.0.2.7;proto=https;host=app.example.test"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "203.0.113.10:https:app.example.test"
        );
    }

    #[tokio::test]
    async fn x_forwarded_family_is_explicit_and_normalizes_a_trusted_chain() {
        let handler = |context: ForwardedContext| async move {
            format!(
                "{}:{}:{}",
                context.client_ip(),
                context.scheme().unwrap(),
                context.host().unwrap()
            )
        };
        let service = TrustedProxyLayer::new(multi_hop_policy())
            .with_x_forwarded()
            .layer(App::new().get("/context", handler));
        let response = service
            .clone()
            .oneshot(x_forwarded_request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("203.0.113.10, 192.0.2.7"),
                Some("https"),
                Some("app.example.test"),
            ))
            .await
            .unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "203.0.113.10:https:app.example.test"
        );
        assert_eq!(
            service
                .oneshot(request(
                    IpAddr::from_str("10.2.3.4").unwrap(),
                    Some("for=203.0.113.10;proto=https;host=app.example.test"),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        let default = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
            "/context",
            |_: ForwardedContext| async move { "unexpected" },
        ));
        assert_eq!(
            default
                .oneshot(x_forwarded_request(
                    IpAddr::from_str("10.2.3.4").unwrap(),
                    Some("203.0.113.10"),
                    Some("https"),
                    Some("app.example.test"),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn x_forwarded_rejects_duplicate_or_incomplete_trusted_headers() {
        let service = TrustedProxyLayer::new(trusted_policy())
            .with_x_forwarded()
            .layer(App::new().get("/context", || async { "unexpected" }));
        let mut duplicate = x_forwarded_request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("203.0.113.10"),
            None,
            None,
        );
        duplicate
            .headers_mut()
            .append(X_FORWARDED_FOR, "203.0.113.11".parse().unwrap());
        assert_eq!(
            service.clone().oneshot(duplicate).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            service
                .oneshot(x_forwarded_request(
                    IpAddr::from_str("10.2.3.4").unwrap(),
                    None,
                    Some("https"),
                    None,
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn x_forwarded_rejects_malformed_scheme_or_host() {
        let service = TrustedProxyLayer::new(trusted_policy())
            .with_x_forwarded()
            .layer(App::new().get("/context", || async { "unexpected" }));
        assert_eq!(
            service
                .clone()
                .oneshot(x_forwarded_request(
                    IpAddr::from_str("10.2.3.4").unwrap(),
                    Some("203.0.113.10"),
                    Some("HTTPS"),
                    None,
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            service
                .oneshot(x_forwarded_request(
                    IpAddr::from_str("10.2.3.4").unwrap(),
                    Some("203.0.113.10"),
                    None,
                    Some("user@app.example.test"),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn single_hop_policy_rejects_chain_or_non_edge_scheme_metadata() {
        let service = TrustedProxyLayer::new(trusted_policy())
            .layer(App::new().get("/context", || async { "unexpected" }));
        let chained = service
            .clone()
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=203.0.113.10, for=192.0.2.7"),
            ))
            .await
            .unwrap();
        assert_eq!(chained.status(), StatusCode::BAD_REQUEST);

        let conflicting = service
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=203.0.113.10;proto=https, for=192.0.2.7"),
            ))
            .await
            .unwrap();
        assert_eq!(conflicting.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn forwarded_chain_hops_are_explicit_and_bounded() {
        assert_eq!(
            trusted_policy().with_forwarded_chain_hops(0).unwrap_err(),
            TrustedProxyError::InvalidForwardedChainHops
        );
        assert_eq!(
            trusted_policy()
                .with_forwarded_chain_hops(MAX_FORWARDED_CHAIN_HOPS + 1)
                .unwrap_err(),
            TrustedProxyError::InvalidForwardedChainHops
        );
    }

    #[tokio::test]
    async fn untrusted_peer_cannot_spoof_forwarded_context() {
        let service = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
            "/context",
            |_: ForwardedContext| async move { "unexpected" },
        ));
        let response = service
            .oneshot(request(
                IpAddr::from_str("198.51.100.7").unwrap(),
                Some("for=203.0.113.10;proto=https;host=app.example.test"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), 500);
    }

    #[tokio::test]
    async fn malformed_header_from_trusted_proxy_is_rejected() {
        let service = TrustedProxyLayer::new(trusted_policy())
            .layer(App::new().get("/context", || async { "unexpected" }));
        let response = service
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=not-an-ip;proto=https"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn duplicate_header_from_trusted_proxy_is_rejected() {
        let service = TrustedProxyLayer::new(trusted_policy())
            .layer(App::new().get("/context", || async { "unexpected" }));
        let mut request = request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10;proto=https"),
        );
        request
            .headers_mut()
            .append(FORWARDED, "for=203.0.113.11;proto=https".parse().unwrap());
        let response = service.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 400);
    }
}
