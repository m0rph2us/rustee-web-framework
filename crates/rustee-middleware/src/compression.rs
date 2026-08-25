//! Streaming HTTP response compression and content-coding negotiation.

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{
    HeaderMap, HeaderValue, Method,
    header::{CONTENT_ENCODING, CONTENT_LENGTH, RANGE},
};
use rustee_core::{BoxCloneServiceExt, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

mod body;
mod negotiation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentCoding {
    Brotli,
    Gzip,
}

impl ContentCoding {
    pub(super) const fn token(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Gzip => "gzip",
        }
    }

    pub(super) const fn header_value(self) -> HeaderValue {
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
        negotiation::select_coding(self.brotli, self.gzip, headers)
    }
}

impl Default for CompressionLayer {
    fn default() -> Self {
        Self::new()
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
        let inner = self.inner.clone();
        let may_transform =
            request.method() != Method::HEAD && !request.headers().contains_key(RANGE);
        let coding = if may_transform {
            self.layer.select_coding(request.headers())
        } else {
            None
        };

        Box::pin(async move {
            let response = inner.call_ready(request).await?;
            Ok(
                if may_transform && negotiation::is_compressible_response(&response) {
                    match coding {
                        Some(coding) => compress_response(response, coding),
                        None => vary_identity_response(response),
                    }
                } else {
                    response
                },
            )
        })
    }
}

fn compress_response(response: Response, coding: ContentCoding) -> Response {
    let (mut parts, body) = response.into_parts();
    parts.headers.remove(CONTENT_LENGTH);
    parts
        .headers
        .insert(CONTENT_ENCODING, coding.header_value());
    negotiation::add_vary_accept_encoding(&mut parts.headers);
    Response::from_parts(parts, body::compressed_body(body, coding))
}

fn vary_identity_response(mut response: Response) -> Response {
    negotiation::add_vary_accept_encoding(response.headers_mut());
    response
}
