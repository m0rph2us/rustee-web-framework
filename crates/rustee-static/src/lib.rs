//! Prefix-bound static file delivery for Rustee Tower services.
//!
//! The layer only handles `GET` and `HEAD` requests under an explicit mount path. It rejects
//! malformed percent encoding, decoded traversal components, directories, oversized files, and
//! canonical paths outside the configured root without revealing filesystem details. GET and HEAD
//! responses include weak file validators for conditional requests. Byte ranges are supported
//! for the identity representation. Opt-in precompressed variants are selected only for
//! non-range requests, and optional bounded streaming avoids buffering large representations.
//! Automatic index files remain a separate contract.

use std::{
    convert::Infallible,
    fs,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode,
    header::{
        ACCEPT_ENCODING, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
        CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE,
        LAST_MODIFIED, RANGE, VARY, X_CONTENT_TYPE_OPTIONS,
    },
};
use rustee_core::{
    Body, Error, IntoResponse, Request, Response, empty_body, full_body, response, stream_body,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tower::{Layer, Service, util::BoxCloneService};

const DEFAULT_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const STREAMING_CHUNK_BYTES: usize = 16 * 1024;
const MAX_MULTIPART_RANGES: usize = 16;
const MAX_RANGE_MEMBERS: usize = 64;

static NEXT_MULTIPART_BOUNDARY: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
struct CacheValidators {
    etag: HeaderValue,
    last_modified: HeaderValue,
    modified: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    const fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RequestedRange {
    Full,
    Partial(ByteRange),
    Multipart(Vec<ByteRange>),
    Unsatisfiable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrecompressedEncoding {
    Brotli,
    Gzip,
}

impl PrecompressedEncoding {
    const fn token(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Gzip => "gzip",
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Brotli => ".br",
            Self::Gzip => ".gz",
        }
    }

    const fn header_value(self) -> HeaderValue {
        match self {
            Self::Brotli => HeaderValue::from_static("br"),
            Self::Gzip => HeaderValue::from_static("gzip"),
        }
    }
}

#[derive(Debug)]
struct StaticRepresentation {
    target: PathBuf,
    metadata: fs::Metadata,
    content_encoding: Option<PrecompressedEncoding>,
}

struct StaticResponseContext<'a> {
    files: &'a StaticFiles,
    representation: &'a StaticRepresentation,
    validators: Option<&'a CacheValidators>,
    content_type_target: &'a Path,
    varies_by_encoding: bool,
}

/// Static file configuration rejected before a layer is created.
#[derive(Debug, thiserror::Error)]
pub enum StaticFilesError {
    /// The configured root could not be canonicalized.
    #[error("static file root could not be canonicalized")]
    RootCanonicalization(#[source] io::Error),
    /// The configured root was not a directory.
    #[error("static file root must be a directory")]
    RootNotDirectory,
    /// The configured mount path was unsafe or not an absolute URI path.
    #[error("static file mount path must be an absolute, normalized URI path")]
    InvalidMountPath,
    /// The maximum response size was zero.
    #[error("static file maximum size must be greater than zero")]
    ZeroMaxFileBytes,
    /// The streaming threshold was zero.
    #[error("static file streaming threshold must be greater than zero")]
    ZeroStreamingThreshold,
}

/// Static files served only below one configured URI mount path.
#[derive(Clone, Debug)]
pub struct StaticFiles {
    root: Arc<PathBuf>,
    mount_path: String,
    max_file_bytes: u64,
    streaming_threshold: Option<u64>,
    cache_control: HeaderValue,
    precompressed_variants: bool,
}

impl StaticFiles {
    /// Canonicalizes an existing static file root with conservative defaults.
    ///
    /// The default mount path is `/static`, the maximum response size is 8 MiB, and responses use
    /// `Cache-Control: no-store` until an application deliberately chooses a cache policy.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError`] when `root` cannot be read or is not a directory.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, StaticFilesError> {
        let root = fs::canonicalize(root).map_err(StaticFilesError::RootCanonicalization)?;
        if !root.is_dir() {
            return Err(StaticFilesError::RootNotDirectory);
        }
        Ok(Self {
            root: Arc::new(root),
            mount_path: String::from("/static"),
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            streaming_threshold: None,
            cache_control: HeaderValue::from_static("no-store"),
            precompressed_variants: false,
        })
    }

    /// Sets the URI mount path handled by this static file layer.
    ///
    /// The mount must be an absolute normalized path such as `/assets`. A root mount is allowed;
    /// use it only when no application route should handle unmatched paths.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::InvalidMountPath`] for a trailing slash, duplicate slash,
    /// query/fragment marker, percent encoding, or traversal-like component.
    pub fn at(mut self, mount_path: impl AsRef<str>) -> Result<Self, StaticFilesError> {
        let mount_path = mount_path.as_ref();
        if !valid_mount_path(mount_path) {
            return Err(StaticFilesError::InvalidMountPath);
        }
        mount_path.clone_into(&mut self.mount_path);
        Ok(self)
    }

    /// Sets the maximum body size for one static file response.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::ZeroMaxFileBytes`] when `max_file_bytes` is zero.
    pub fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Result<Self, StaticFilesError> {
        if max_file_bytes == 0 {
            return Err(StaticFilesError::ZeroMaxFileBytes);
        }
        self.max_file_bytes = max_file_bytes;
        Ok(self)
    }

    /// Streams successful file bodies at or above one representation-size threshold.
    ///
    /// Streaming reads the selected full or single-range representation in bounded chunks. The
    /// configured [`StaticFiles::with_max_file_bytes`] limit remains the admission boundary, so
    /// applications must raise that limit deliberately before serving larger files.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::ZeroStreamingThreshold`] when `threshold` is zero.
    pub fn with_streaming_threshold(mut self, threshold: u64) -> Result<Self, StaticFilesError> {
        if threshold == 0 {
            return Err(StaticFilesError::ZeroStreamingThreshold);
        }
        self.streaming_threshold = Some(threshold);
        Ok(self)
    }

    /// Sets the exact `Cache-Control` header used for successful static file responses.
    #[must_use]
    pub fn with_cache_control(mut self, cache_control: HeaderValue) -> Self {
        self.cache_control = cache_control;
        self
    }

    /// Enables selection of sibling `.br` and `.gz` files through `Accept-Encoding`.
    ///
    /// Variants are served only for requests without `Range`; range requests keep the identity
    /// representation and its existing conditional cache contract.
    #[must_use]
    pub const fn with_precompressed_variants(mut self, enabled: bool) -> Self {
        self.precompressed_variants = enabled;
        self
    }

    /// Returns a Tower layer that serves this configuration before its inner service.
    pub fn layer(self) -> StaticFilesLayer {
        StaticFilesLayer { files: self }
    }

    fn relative_path(&self, path: &str) -> Result<Option<PathBuf>, ()> {
        let relative = if self.mount_path == "/" {
            path.strip_prefix('/').ok_or(())?
        } else if path == self.mount_path {
            ""
        } else {
            let prefix = self.mount_path.clone() + "/";
            let Some(relative) = path.strip_prefix(&prefix) else {
                return Ok(None);
            };
            relative
        };

        if relative.is_empty() {
            return Ok(Some(PathBuf::new()));
        }

        let mut local = PathBuf::new();
        for segment in relative.split('/') {
            let segment = percent_decode_segment(segment)?;
            local.push(segment);
        }
        Ok(Some(local))
    }
}

/// Tower layer produced by [`StaticFiles::layer`].
#[derive(Clone, Debug)]
#[must_use = "a static file layer must be applied to a service to have an effect"]
pub struct StaticFilesLayer {
    files: StaticFiles,
}

/// Service produced by [`StaticFilesLayer`].
#[derive(Clone, Debug)]
pub struct StaticFilesService {
    inner: BoxCloneService<Request, Response, Infallible>,
    files: StaticFiles,
}

impl<S> Layer<S> for StaticFilesLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = StaticFilesService;

    fn layer(&self, inner: S) -> Self::Service {
        StaticFilesService {
            inner: BoxCloneService::new(inner),
            files: self.files.clone(),
        }
    }
}

impl Service<Request> for StaticFilesService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        let files = self.files.clone();
        Box::pin(async move {
            if !matches!(request.method(), &Method::GET | &Method::HEAD) {
                return inner.call(request).await;
            }
            let relative = match files.relative_path(request.uri().path()) {
                Ok(None) => return inner.call(request).await,
                Ok(Some(relative)) => relative,
                Err(()) => return Ok(static_not_found()),
            };
            Ok(serve_file(
                files,
                relative,
                request.method() == Method::HEAD,
                request.headers(),
            )
            .await)
        })
    }
}

async fn serve_file(
    files: StaticFiles,
    relative: PathBuf,
    head: bool,
    request_headers: &HeaderMap,
) -> Response {
    if relative.as_os_str().is_empty() {
        return static_not_found();
    }
    let target = files.root.join(relative);
    let target = match tokio::fs::canonicalize(target).await {
        Ok(target) if target.starts_with(files.root.as_ref()) => target,
        _ => return static_not_found(),
    };
    let metadata = match tokio::fs::metadata(&target).await {
        Ok(metadata) if metadata.is_file() && metadata.len() <= files.max_file_bytes => metadata,
        _ => return static_not_found(),
    };
    let content_type_target = target.clone();
    let varies_by_encoding = files.precompressed_variants && !request_headers.contains_key(RANGE);
    let mut representation = StaticRepresentation {
        target,
        metadata,
        content_encoding: None,
    };
    if varies_by_encoding
        && let Some(variant) =
            select_precompressed_variant(&files, &representation.target, request_headers).await
    {
        representation = variant;
    }
    let validators = cache_validators(&representation.metadata);
    if is_not_modified(request_headers, validators.as_ref()) {
        return static_not_modified(
            &files,
            validators.as_ref(),
            representation.content_encoding,
            varies_by_encoding,
        );
    }
    let requested_range = match requested_range(
        request_headers,
        representation.metadata.len(),
        validators.as_ref(),
    ) {
        RequestedRange::Unsatisfiable => {
            return static_range_not_satisfiable(
                &files,
                validators.as_ref(),
                representation.metadata.len(),
            );
        }
        requested_range => requested_range,
    };

    let context = StaticResponseContext {
        files: &files,
        representation: &representation,
        validators: validators.as_ref(),
        content_type_target: &content_type_target,
        varies_by_encoding,
    };
    if let RequestedRange::Multipart(ranges) = &requested_range {
        let boundary = multipart_boundary();
        let part_content_type = content_type(&content_type_target);
        let Some(length) = multipart_content_length(
            &boundary,
            part_content_type.as_bytes(),
            ranges,
            representation.metadata.len(),
        ) else {
            return static_range_not_satisfiable(
                &files,
                validators.as_ref(),
                representation.metadata.len(),
            );
        };
        let body = if head {
            empty_body()
        } else {
            multipart_file_body(
                representation.target.clone(),
                &part_content_type,
                boundary.clone(),
                ranges.clone(),
                representation.metadata.len(),
            )
        };
        return static_multipart_response(&context, body, length, &boundary);
    }

    let Some((status, range, body)) = read_static_body(
        &representation,
        requested_range,
        head,
        files.streaming_threshold,
    )
    .await
    else {
        return static_not_found();
    };
    let length = range.map_or(representation.metadata.len(), ByteRange::len);
    static_success_response(&context, status, body, length, range)
}

async fn read_static_body(
    representation: &StaticRepresentation,
    requested_range: RequestedRange,
    head: bool,
    streaming_threshold: Option<u64>,
) -> Option<(StatusCode, Option<ByteRange>, Body)> {
    match requested_range {
        RequestedRange::Full => {
            let body = if head {
                empty_body()
            } else {
                static_file_body(
                    &representation.target,
                    0,
                    representation.metadata.len(),
                    streaming_threshold,
                    true,
                )
                .await?
            };
            Some((StatusCode::OK, None, body))
        }
        RequestedRange::Partial(range) => {
            let body = if head {
                empty_body()
            } else {
                static_file_body(
                    &representation.target,
                    range.start,
                    range.len(),
                    streaming_threshold,
                    false,
                )
                .await?
            };
            Some((StatusCode::PARTIAL_CONTENT, Some(range), body))
        }
        RequestedRange::Multipart(_) | RequestedRange::Unsatisfiable => None,
    }
}

async fn static_file_body(
    target: &Path,
    offset: u64,
    length: u64,
    streaming_threshold: Option<u64>,
    full_representation: bool,
) -> Option<Body> {
    if streaming_threshold.is_some_and(|threshold| length >= threshold) {
        return streaming_file_body(target, offset, length).await;
    }
    if full_representation {
        let bytes = tokio::fs::read(target).await.ok()?;
        return (u64::try_from(bytes.len()).ok()? == length).then(|| full_body(bytes));
    }
    read_file_range(target, offset, length).await.map(full_body)
}

async fn streaming_file_body(target: &Path, offset: u64, length: u64) -> Option<Body> {
    let mut file = tokio::fs::File::open(target).await.ok()?;
    file.seek(SeekFrom::Start(offset)).await.ok()?;
    let stream = ReaderStream::with_capacity(file.take(length), STREAMING_CHUNK_BYTES);
    Some(stream_body(stream))
}

async fn read_file_range(target: &Path, offset: u64, length: u64) -> Option<Vec<u8>> {
    let mut file = tokio::fs::File::open(target).await.ok()?;
    file.seek(SeekFrom::Start(offset)).await.ok()?;
    let mut bytes = vec![0; usize::try_from(length).ok()?];
    file.read_exact(&mut bytes).await.ok()?;
    Some(bytes)
}

fn static_success_response(
    context: &StaticResponseContext<'_>,
    status: StatusCode,
    body: Body,
    length: u64,
    range: Option<ByteRange>,
) -> Response {
    let mut response = response(status, body);
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, content_type(context.content_type_target));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    headers.insert(CACHE_CONTROL, context.files.cache_control.clone());
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(range) = range {
        headers.insert(
            CONTENT_RANGE,
            content_range_header(range, context.representation.metadata.len()),
        );
    }
    if let Some(validators) = context.validators {
        insert_validators(headers, validators);
    }
    apply_representation_headers(
        headers,
        context.representation.content_encoding,
        context.varies_by_encoding,
    );
    response
}

fn static_multipart_response(
    context: &StaticResponseContext<'_>,
    body: Body,
    length: u64,
    boundary: &str,
) -> Response {
    let mut response = response(StatusCode::PARTIAL_CONTENT, body);
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/byteranges; boundary={boundary}"))
            .expect("generated multipart content type is a valid header"),
    );
    headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    headers.insert(CACHE_CONTROL, context.files.cache_control.clone());
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(validators) = context.validators {
        insert_validators(headers, validators);
    }
    response
}

fn multipart_boundary() -> String {
    let counter = NEXT_MULTIPART_BOUNDARY.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("rustee-{timestamp:x}-{counter:x}")
}

fn multipart_content_length(
    boundary: &str,
    content_type: &[u8],
    ranges: &[ByteRange],
    full_length: u64,
) -> Option<u64> {
    let mut length = 0_u64;
    for range in ranges {
        let part_header = multipart_part_header(boundary, content_type, *range, full_length);
        length = length
            .checked_add(u64::try_from(part_header.len()).ok()?)?
            .checked_add(range.len())?
            .checked_add(2)?;
    }
    length.checked_add(u64::try_from(multipart_closing_boundary(boundary).len()).ok()?)
}

fn multipart_file_body(
    target: PathBuf,
    content_type: &HeaderValue,
    boundary: String,
    ranges: Vec<ByteRange>,
    full_length: u64,
) -> Body {
    let content_type = content_type.as_bytes().to_vec();
    let stream = async_stream::stream! {
        for range in ranges {
            yield Ok::<Bytes, io::Error>(multipart_part_header(
                &boundary,
                &content_type,
                range,
                full_length,
            ));
            let mut file = match tokio::fs::File::open(&target).await {
                Ok(file) => file,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            if let Err(error) = file.seek(SeekFrom::Start(range.start)).await {
                yield Err(error);
                return;
            }
            let stream = ReaderStream::with_capacity(file.take(range.len()), STREAMING_CHUNK_BYTES);
            futures_util::pin_mut!(stream);
            while let Some(chunk) = stream.next().await {
                yield chunk;
            }
            yield Ok(Bytes::from_static(b"\r\n"));
        }
        yield Ok(Bytes::from(multipart_closing_boundary(&boundary)));
    };
    stream_body(stream)
}

fn multipart_part_header(
    boundary: &str,
    content_type: &[u8],
    range: ByteRange,
    full_length: u64,
) -> Bytes {
    let content_type = std::str::from_utf8(content_type)
        .expect("Rustee static content types are valid ASCII headers");
    Bytes::from(format!(
        "--{boundary}\r\nContent-Type: {content_type}\r\nContent-Range: bytes {}-{}/{full_length}\r\n\r\n",
        range.start, range.end
    ))
}

fn multipart_closing_boundary(boundary: &str) -> String {
    format!("--{boundary}--\r\n")
}

async fn select_precompressed_variant(
    files: &StaticFiles,
    target: &Path,
    request_headers: &HeaderMap,
) -> Option<StaticRepresentation> {
    for encoding in preferred_precompressed_encodings(request_headers) {
        let candidate = variant_path(target, encoding);
        let Ok(target) = tokio::fs::canonicalize(candidate).await else {
            continue;
        };
        if !target.starts_with(files.root.as_ref()) {
            continue;
        }
        let Ok(metadata) = tokio::fs::metadata(&target).await else {
            continue;
        };
        if metadata.is_file() && metadata.len() <= files.max_file_bytes {
            return Some(StaticRepresentation {
                target,
                metadata,
                content_encoding: Some(encoding),
            });
        }
    }
    None
}

fn preferred_precompressed_encodings(headers: &HeaderMap) -> Vec<PrecompressedEncoding> {
    let brotli = encoding_quality(headers, PrecompressedEncoding::Brotli);
    let gzip = encoding_quality(headers, PrecompressedEncoding::Gzip);
    match (brotli, gzip) {
        (Some(brotli), Some(gzip)) if brotli >= gzip => {
            vec![PrecompressedEncoding::Brotli, PrecompressedEncoding::Gzip]
        }
        (Some(_), Some(_)) => vec![PrecompressedEncoding::Gzip, PrecompressedEncoding::Brotli],
        (Some(_), None) => vec![PrecompressedEncoding::Brotli],
        (None, Some(_)) => vec![PrecompressedEncoding::Gzip],
        (None, None) => Vec::new(),
    }
}

fn encoding_quality(headers: &HeaderMap, encoding: PrecompressedEncoding) -> Option<u16> {
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

            if token.eq_ignore_ascii_case(encoding.token()) {
                explicit = Some(explicit.unwrap_or(0).max(quality));
            } else if token == "*" {
                wildcard = Some(wildcard.unwrap_or(0).max(quality));
            }
        }
    }

    found
        .then_some(explicit.or(wildcard).filter(|quality| *quality > 0))
        .flatten()
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

fn variant_path(target: &Path, encoding: PrecompressedEncoding) -> PathBuf {
    let mut path = target.as_os_str().to_os_string();
    path.push(encoding.suffix());
    PathBuf::from(path)
}

fn cache_validators(metadata: &fs::Metadata) -> Option<CacheValidators> {
    let modified = metadata.modified().ok()?;
    let since_epoch = modified.duration_since(UNIX_EPOCH).ok()?;
    let etag = HeaderValue::from_str(&format!(
        "W/\"{:x}-{:x}-{:x}\"",
        metadata.len(),
        since_epoch.as_secs(),
        since_epoch.subsec_nanos()
    ))
    .ok()?;
    let last_modified = HeaderValue::from_str(&httpdate::fmt_http_date(modified)).ok()?;
    Some(CacheValidators {
        etag,
        last_modified,
        modified,
    })
}

fn is_not_modified(headers: &HeaderMap, validators: Option<&CacheValidators>) -> bool {
    match if_none_match(headers, validators) {
        Some(matches) => matches,
        None => {
            validators.is_some_and(|validators| if_modified_since(headers, validators.modified))
        }
    }
}

fn requested_range(
    headers: &HeaderMap,
    length: u64,
    validators: Option<&CacheValidators>,
) -> RequestedRange {
    let values = headers.get_all(RANGE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return RequestedRange::Full;
    };
    if values.next().is_some() {
        return RequestedRange::Unsatisfiable;
    }
    let Ok(value) = value.to_str() else {
        return RequestedRange::Unsatisfiable;
    };
    let Some(specifier) = value.trim().strip_prefix("bytes=") else {
        return RequestedRange::Unsatisfiable;
    };
    if !if_range_matches(headers, validators) {
        return RequestedRange::Full;
    }
    parse_range_set(specifier.trim(), length)
}

fn parse_range_set(specifier: &str, length: u64) -> RequestedRange {
    let mut ranges = Vec::new();
    for (index, specifier) in specifier.split(',').enumerate() {
        if index >= MAX_RANGE_MEMBERS {
            return RequestedRange::Unsatisfiable;
        }
        let range = match parse_range_member(specifier.trim(), length) {
            Ok(Some(range)) => range,
            Ok(None) => continue,
            Err(()) => return RequestedRange::Unsatisfiable,
        };
        ranges.push(range);
    }
    if ranges.is_empty() {
        return RequestedRange::Unsatisfiable;
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut normalized: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start <= previous.end.saturating_add(1)
        {
            previous.end = previous.end.max(range.end);
        } else {
            normalized.push(range);
        }
    }
    if normalized.len() > MAX_MULTIPART_RANGES {
        return RequestedRange::Unsatisfiable;
    }
    match normalized.as_slice() {
        [range] => RequestedRange::Partial(*range),
        _ => RequestedRange::Multipart(normalized),
    }
}

fn parse_range_member(specifier: &str, length: u64) -> Result<Option<ByteRange>, ()> {
    let Some((start, end)) = specifier.split_once('-') else {
        return Err(());
    };
    if start.is_empty() {
        let suffix_length = end.parse::<u64>().map_err(|_| ())?;
        if suffix_length == 0 || length == 0 {
            return Ok(None);
        }
        let start = length.saturating_sub(suffix_length);
        return Ok(Some(ByteRange {
            start,
            end: length - 1,
        }));
    }

    let start = start.parse::<u64>().map_err(|_| ())?;
    if start >= length {
        return Ok(None);
    }
    let end = if end.is_empty() {
        length - 1
    } else {
        let end = end.parse::<u64>().map_err(|_| ())?;
        end.min(length - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some(ByteRange { start, end }))
}

fn if_range_matches(headers: &HeaderMap, validators: Option<&CacheValidators>) -> bool {
    let values = headers.get_all(IF_RANGE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    let Some(validators) = validators else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(date) = httpdate::parse_http_date(value) else {
        return false;
    };
    truncate_to_seconds(validators.modified) == Some(date)
}

fn if_none_match(headers: &HeaderMap, validators: Option<&CacheValidators>) -> Option<bool> {
    let values = headers.get_all(IF_NONE_MATCH);
    values.iter().next()?;
    let Some(validators) = validators else {
        return Some(
            values
                .iter()
                .any(|value| value.as_bytes().trim_ascii() == b"*"),
        );
    };
    let Ok(current) = validators.etag.to_str() else {
        return Some(false);
    };
    Some(values.iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || weak_etag_matches(candidate, current))
        })
    }))
}

fn weak_etag_matches(candidate: &str, current: &str) -> bool {
    strip_weak_prefix(candidate) == strip_weak_prefix(current)
}

fn strip_weak_prefix(value: &str) -> &str {
    value.trim().strip_prefix("W/").unwrap_or(value.trim())
}

fn if_modified_since(headers: &HeaderMap, modified: SystemTime) -> bool {
    let values = headers.get_all(IF_MODIFIED_SINCE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(since) = httpdate::parse_http_date(value) else {
        return false;
    };
    truncate_to_seconds(modified).is_some_and(|modified| modified <= since)
}

fn truncate_to_seconds(value: SystemTime) -> Option<SystemTime> {
    let duration = value.duration_since(UNIX_EPOCH).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(duration.as_secs()))
}

fn static_not_modified(
    files: &StaticFiles,
    validators: Option<&CacheValidators>,
    content_encoding: Option<PrecompressedEncoding>,
    varies_by_encoding: bool,
) -> Response {
    let mut response = response(StatusCode::NOT_MODIFIED, empty_body());
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, files.cache_control.clone());
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(validators) = validators {
        insert_validators(headers, validators);
    }
    apply_representation_headers(headers, content_encoding, varies_by_encoding);
    response
}

fn static_range_not_satisfiable(
    files: &StaticFiles,
    validators: Option<&CacheValidators>,
    length: u64,
) -> Response {
    let mut response = response(StatusCode::RANGE_NOT_SATISFIABLE, empty_body());
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, files.cache_control.clone());
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(CONTENT_RANGE, unsatisfied_content_range_header(length));
    if let Some(validators) = validators {
        insert_validators(headers, validators);
    }
    response
}

fn content_range_header(range: ByteRange, length: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("bytes {}-{}/{length}", range.start, range.end))
        .expect("formatted content range is a valid header")
}

fn unsatisfied_content_range_header(length: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("bytes */{length}"))
        .expect("formatted content range is a valid header")
}

fn insert_validators(headers: &mut HeaderMap, validators: &CacheValidators) {
    headers.insert(ETAG, validators.etag.clone());
    headers.insert(LAST_MODIFIED, validators.last_modified.clone());
}

fn apply_representation_headers(
    headers: &mut HeaderMap,
    content_encoding: Option<PrecompressedEncoding>,
    varies_by_encoding: bool,
) {
    if let Some(content_encoding) = content_encoding {
        headers.insert(CONTENT_ENCODING, content_encoding.header_value());
    }
    if varies_by_encoding {
        add_vary_accept_encoding(headers);
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

fn static_not_found() -> Response {
    Error::not_found("the requested static resource was not found").into_response()
}

fn valid_mount_path(path: &str) -> bool {
    if !path.starts_with('/')
        || path.contains(['?', '#', '%'])
        || path.contains("//")
        || (path.len() > 1 && path.ends_with('/'))
    {
        return false;
    }
    path == "/" || path.split('/').skip(1).all(valid_mount_segment)
}

fn valid_mount_segment(segment: &str) -> bool {
    !segment.is_empty() && !matches!(segment, "." | "..") && !segment.contains(['\\', '\0'])
}

fn percent_decode_segment(segment: &str) -> Result<String, ()> {
    if segment.is_empty() {
        return Err(());
    }
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1).ok_or(())?;
            let low = *bytes.get(index + 2).ok_or(())?;
            decoded.push((hex_value(high)? << 4) | hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let decoded = String::from_utf8(decoded).map_err(|_| ())?;
    if !valid_mount_segment(&decoded) || decoded.contains('/') {
        return Err(());
    }
    Ok(decoded)
}

fn hex_value(value: u8) -> Result<u8, ()> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(()),
    }
}

fn content_type(path: &Path) -> HeaderValue {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("css") => HeaderValue::from_static("text/css; charset=utf-8"),
        Some("gif") => HeaderValue::from_static("image/gif"),
        Some("htm" | "html") => HeaderValue::from_static("text/html; charset=utf-8"),
        Some("ico") => HeaderValue::from_static("image/x-icon"),
        Some("jpeg" | "jpg") => HeaderValue::from_static("image/jpeg"),
        Some("js" | "mjs") => HeaderValue::from_static("text/javascript; charset=utf-8"),
        Some("json" | "map") => HeaderValue::from_static("application/json; charset=utf-8"),
        Some("png") => HeaderValue::from_static("image/png"),
        Some("svg") => HeaderValue::from_static("image/svg+xml"),
        Some("txt") => HeaderValue::from_static("text/plain; charset=utf-8"),
        Some("wasm") => HeaderValue::from_static("application/wasm"),
        Some("webp") => HeaderValue::from_static("image/webp"),
        _ => HeaderValue::from_static("application/octet-stream"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use http::{
        Method, Request as HttpRequest, StatusCode,
        header::{
            ACCEPT_ENCODING, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
            CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE,
            LAST_MODIFIED, RANGE, VARY,
        },
    };
    use http_body_util::BodyExt;
    use rustee_core::{IntoResponse, empty_body};
    use tower::{Layer, ServiceExt, service_fn, util::BoxCloneService};

    use super::{MAX_RANGE_MEMBERS, STREAMING_CHUNK_BYTES, StaticFiles, StaticFilesError};

    static NEXT_TEMP_ROOT: AtomicU64 = AtomicU64::new(0);

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = NEXT_TEMP_ROOT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "rustee-static-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn request(method: Method, uri: &str) -> rustee_core::Request {
        HttpRequest::builder()
            .method(method)
            .uri(uri)
            .body(empty_body())
            .unwrap()
    }

    fn fallback_service()
    -> BoxCloneService<rustee_core::Request, rustee_core::Response, std::convert::Infallible> {
        BoxCloneService::new(service_fn(|_| async {
            Ok::<_, std::convert::Infallible>((StatusCode::IM_A_TEAPOT, "fallback").into_response())
        }))
    }

    #[tokio::test]
    async fn serves_get_and_head_with_explicit_safe_headers() {
        let root = TempRoot::new();
        fs::write(root.path().join("app.css"), "body { color: black; }").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .layer()
            .layer(fallback_service());

        let response = service
            .clone()
            .oneshot(request(Method::GET, "/assets/app.css"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/css; charset=utf-8");
        assert_eq!(response.headers()["content-length"], "22");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "body { color: black; }"
        );

        let response = service
            .oneshot(request(Method::HEAD, "/assets/app.css"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-length"], "22");
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );
    }

    #[test]
    fn rejects_a_zero_streaming_threshold() {
        let root = TempRoot::new();
        assert!(matches!(
            StaticFiles::new(root.path())
                .unwrap()
                .with_streaming_threshold(0),
            Err(StaticFilesError::ZeroStreamingThreshold)
        ));
    }

    #[tokio::test]
    async fn conditional_requests_use_weak_validators_and_precedence() {
        let root = TempRoot::new();
        fs::write(root.path().join("app.css"), "body { color: black; }").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_cache_control("public, max-age=60".parse().unwrap())
            .layer()
            .layer(fallback_service());

        let initial = service
            .clone()
            .oneshot(request(Method::GET, "/assets/app.css"))
            .await
            .unwrap();
        let etag = initial.headers()[ETAG].clone();
        let last_modified = initial.headers()[LAST_MODIFIED].clone();
        assert!(etag.to_str().unwrap().starts_with("W/\""));
        assert_eq!(
            initial.into_body().collect().await.unwrap().to_bytes(),
            "body { color: black; }"
        );

        let mut if_none_match = request(Method::GET, "/assets/app.css");
        if_none_match
            .headers_mut()
            .insert(IF_NONE_MATCH, etag.clone());
        let response = service.clone().oneshot(if_none_match).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[ETAG], etag);
        assert_eq!(response.headers()[LAST_MODIFIED], last_modified);
        assert_eq!(response.headers()[CACHE_CONTROL], "public, max-age=60");
        assert!(response.headers().get(CONTENT_TYPE).is_none());
        assert!(response.headers().get(CONTENT_LENGTH).is_none());
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let mut weak_comparison = request(Method::HEAD, "/assets/app.css");
        weak_comparison.headers_mut().insert(
            IF_NONE_MATCH,
            etag.to_str()
                .unwrap()
                .strip_prefix("W/")
                .unwrap()
                .parse()
                .unwrap(),
        );
        assert_eq!(
            service
                .clone()
                .oneshot(weak_comparison)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_MODIFIED
        );

        let mut if_modified_since = request(Method::GET, "/assets/app.css");
        if_modified_since
            .headers_mut()
            .insert(IF_MODIFIED_SINCE, last_modified.clone());
        assert_eq!(
            service
                .clone()
                .oneshot(if_modified_since)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_MODIFIED
        );

        let mut etag_precedence = request(Method::GET, "/assets/app.css");
        etag_precedence
            .headers_mut()
            .insert(IF_NONE_MATCH, "W/\"not-this-version\"".parse().unwrap());
        etag_precedence
            .headers_mut()
            .insert(IF_MODIFIED_SINCE, last_modified);
        let response = service.oneshot(etag_precedence).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "body { color: black; }"
        );
    }

    #[tokio::test]
    async fn single_byte_ranges_preserve_static_response_headers() {
        let root = TempRoot::new();
        fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_cache_control("public, max-age=60".parse().unwrap())
            .layer()
            .layer(fallback_service());

        let mut partial = request(Method::GET, "/assets/sequence.txt");
        partial
            .headers_mut()
            .insert(RANGE, "bytes=2-5".parse().unwrap());
        let response = service.clone().oneshot(partial).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(response.headers()[CONTENT_LENGTH], "4");
        assert_eq!(response.headers()[CACHE_CONTROL], "public, max-age=60");
        assert!(response.headers().contains_key(ETAG));
        assert!(response.headers().contains_key(LAST_MODIFIED));
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "2345"
        );

        let mut suffix_head = request(Method::HEAD, "/assets/sequence.txt");
        suffix_head
            .headers_mut()
            .insert(RANGE, "bytes=-3".parse().unwrap());
        let response = service.clone().oneshot(suffix_head).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[CONTENT_RANGE], "bytes 7-9/10");
        assert_eq!(response.headers()[CONTENT_LENGTH], "3");
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let mut bounded_end = request(Method::GET, "/assets/sequence.txt");
        bounded_end
            .headers_mut()
            .insert(RANGE, "bytes=8-100".parse().unwrap());
        let response = service.clone().oneshot(bounded_end).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[CONTENT_RANGE], "bytes 8-9/10");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "89"
        );

        let mut unsatisfiable = request(Method::GET, "/assets/sequence.txt");
        unsatisfiable
            .headers_mut()
            .insert(RANGE, "bytes=50-".parse().unwrap());
        let response = service.clone().oneshot(unsatisfiable).await.unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[CONTENT_RANGE], "bytes */10");
        assert!(response.headers().get(CONTENT_LENGTH).is_none());
    }

    #[tokio::test]
    async fn multipart_ranges_normalize_and_keep_their_header_contract() {
        let root = TempRoot::new();
        fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .layer()
            .layer(fallback_service());

        let mut multiple = request(Method::GET, "/assets/sequence.txt");
        multiple
            .headers_mut()
            .insert(RANGE, "bytes=8-9,0-1,1-3".parse().unwrap());
        let response = service.clone().oneshot(multiple).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(response.headers().get(CONTENT_RANGE).is_none());
        let multipart_content_type = response.headers()[CONTENT_TYPE].to_str().unwrap();
        let boundary = multipart_content_type
            .strip_prefix("multipart/byteranges; boundary=")
            .unwrap()
            .to_owned();
        let content_length: usize = response.headers()[CONTENT_LENGTH]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), content_length);
        assert_eq!(
            body,
            format!(
                "--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 0-3/10\r\n\r\n0123\r\n--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 8-9/10\r\n\r\n89\r\n--{boundary}--\r\n"
            )
        );

        let mut multiple_head = request(Method::HEAD, "/assets/sequence.txt");
        multiple_head
            .headers_mut()
            .insert(RANGE, "bytes=0-1,8-9".parse().unwrap());
        let response = service.oneshot(multiple_head).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("multipart/byteranges; boundary=")
        );
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn malformed_or_excessive_range_sets_are_unsatisfiable() {
        let root = TempRoot::new();
        fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .layer()
            .layer(fallback_service());

        let mut malformed = request(Method::GET, "/assets/sequence.txt");
        malformed
            .headers_mut()
            .insert(RANGE, "bytes=0-1,not-a-range".parse().unwrap());
        let response = service.clone().oneshot(malformed).await.unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[CONTENT_RANGE], "bytes */10");

        let ranges = (0..=MAX_RANGE_MEMBERS)
            .map(|index| format!("{index}-{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut excessive = request(Method::GET, "/assets/sequence.txt");
        excessive
            .headers_mut()
            .insert(RANGE, ranges.parse().unwrap());
        assert_eq!(
            service.oneshot(excessive).await.unwrap().status(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
    }

    #[tokio::test]
    async fn streams_large_full_and_single_range_representations_in_bounded_chunks() {
        let root = TempRoot::new();
        let asset = vec![b'x'; STREAMING_CHUNK_BYTES * 2 + 97];
        fs::write(root.path().join("large.txt"), &asset).unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_max_file_bytes(u64::try_from(asset.len()).unwrap())
            .unwrap()
            .with_streaming_threshold(1_024)
            .unwrap()
            .layer()
            .layer(fallback_service());

        let response = service
            .clone()
            .oneshot(request(Method::GET, "/assets/large.txt"))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_LENGTH], asset.len().to_string());
        let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
        assert!(chunk_sizes.len() >= 2);
        assert!(
            chunk_sizes
                .iter()
                .all(|size| *size <= STREAMING_CHUNK_BYTES)
        );
        assert_eq!(body, asset);

        let mut range = request(Method::GET, "/assets/large.txt");
        range.headers_mut().insert(
            RANGE,
            format!("bytes=512-{}", asset.len() - 513).parse().unwrap(),
        );
        let response = service.oneshot(range).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            (asset.len() - 1_024).to_string()
        );
        let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
        assert!(chunk_sizes.len() >= 2);
        assert!(
            chunk_sizes
                .iter()
                .all(|size| *size <= STREAMING_CHUNK_BYTES)
        );
        assert_eq!(body, asset[512..asset.len() - 512]);
    }

    #[tokio::test]
    async fn streams_a_selected_precompressed_variant() {
        let root = TempRoot::new();
        let identity = b"identity asset";
        let variant = vec![b'b'; STREAMING_CHUNK_BYTES + 23];
        fs::write(root.path().join("app.js"), identity).unwrap();
        fs::write(root.path().join("app.js.br"), &variant).unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_max_file_bytes(u64::try_from(variant.len()).unwrap())
            .unwrap()
            .with_streaming_threshold(1_024)
            .unwrap()
            .with_precompressed_variants(true)
            .layer()
            .layer(fallback_service());

        let response = service.oneshot(precompressed_request("br")).await.unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "br");
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            variant.len().to_string()
        );
        let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
        assert!(chunk_sizes.len() >= 2);
        assert!(
            chunk_sizes
                .iter()
                .all(|size| *size <= STREAMING_CHUNK_BYTES)
        );
        assert_eq!(body, variant);
    }

    #[tokio::test]
    async fn range_conditions_prefer_not_modified_and_require_date_if_range() {
        let root = TempRoot::new();
        fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .layer()
            .layer(fallback_service());

        let initial = service
            .clone()
            .oneshot(request(Method::GET, "/assets/sequence.txt"))
            .await
            .unwrap();
        let etag = initial.headers()[ETAG].clone();
        let last_modified = initial.headers()[LAST_MODIFIED].clone();

        let mut date_match = request(Method::GET, "/assets/sequence.txt");
        date_match
            .headers_mut()
            .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
        date_match
            .headers_mut()
            .insert(IF_RANGE, last_modified.clone());
        let response = service.clone().oneshot(date_match).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("multipart/byteranges; boundary=")
        );

        let mut weak_etag = request(Method::GET, "/assets/sequence.txt");
        weak_etag
            .headers_mut()
            .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
        weak_etag.headers_mut().insert(IF_RANGE, etag.clone());
        let response = service.clone().oneshot(weak_etag).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "0123456789"
        );

        let mut not_modified = request(Method::GET, "/assets/sequence.txt");
        not_modified
            .headers_mut()
            .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
        not_modified.headers_mut().insert(IF_NONE_MATCH, etag);
        assert_eq!(
            service.oneshot(not_modified).await.unwrap().status(),
            StatusCode::NOT_MODIFIED
        );
    }

    #[tokio::test]
    async fn precompressed_variants_are_opt_in_and_negotiate_content_coding() {
        let root = TempRoot::new();
        fs::write(root.path().join("app.js"), "identity asset").unwrap();
        fs::write(root.path().join("app.js.br"), "brotli asset").unwrap();
        fs::write(root.path().join("app.js.gz"), "gzip asset").unwrap();

        let identity = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .layer()
            .layer(fallback_service());
        let response = identity
            .oneshot(precompressed_request("br, gzip"))
            .await
            .unwrap();
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert!(response.headers().get(VARY).is_none());
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "identity asset"
        );

        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_precompressed_variants(true)
            .layer()
            .layer(fallback_service());
        let response = service
            .clone()
            .oneshot(precompressed_request("br;q=0.9, gzip"))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
        assert_eq!(response.headers()[VARY], "Accept-Encoding");
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "text/javascript; charset=utf-8"
        );
        assert_eq!(response.headers()[CONTENT_LENGTH], "10");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "gzip asset"
        );

        fs::remove_file(root.path().join("app.js.br")).unwrap();
        let response = service
            .clone()
            .oneshot(precompressed_request("br, gzip;q=0.5"))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "gzip asset"
        );

        let response = service
            .oneshot(precompressed_request("identity"))
            .await
            .unwrap();
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert_eq!(response.headers()[VARY], "Accept-Encoding");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "identity asset"
        );
    }

    #[tokio::test]
    async fn precompressed_variants_keep_variant_validators_and_range_identity() {
        let root = TempRoot::new();
        fs::write(root.path().join("app.js"), "identity asset").unwrap();
        fs::write(root.path().join("app.js.br"), "brotli asset").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_precompressed_variants(true)
            .layer()
            .layer(fallback_service());

        let initial = service
            .clone()
            .oneshot(precompressed_request("br"))
            .await
            .unwrap();
        let etag = initial.headers()[ETAG].clone();
        assert_eq!(initial.headers()[CONTENT_ENCODING], "br");

        let mut not_modified = precompressed_request("br");
        not_modified.headers_mut().insert(IF_NONE_MATCH, etag);
        let response = service.clone().oneshot(not_modified).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[CONTENT_ENCODING], "br");
        assert_eq!(response.headers()[VARY], "Accept-Encoding");

        let mut range = precompressed_request("br");
        range
            .headers_mut()
            .insert(RANGE, "bytes=0-3,5-7".parse().unwrap());
        let response = service.oneshot(range).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert!(response.headers().get(VARY).is_none());
        assert!(response.headers().get(CONTENT_RANGE).is_none());
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("multipart/byteranges; boundary=")
        );
    }

    fn precompressed_request(accept_encoding: &str) -> rustee_core::Request {
        HttpRequest::builder()
            .method(Method::GET)
            .uri("/assets/app.js")
            .header(ACCEPT_ENCODING, accept_encoding)
            .body(empty_body())
            .unwrap()
    }

    async fn collect_data_chunks(mut body: rustee_core::Body) -> (Vec<usize>, Vec<u8>) {
        let mut chunk_sizes = Vec::new();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.unwrap();
            if let Ok(data) = frame.into_data() {
                chunk_sizes.push(data.len());
                bytes.extend_from_slice(data.as_ref());
            }
        }
        (chunk_sizes, bytes)
    }

    #[tokio::test]
    async fn confines_the_mount_and_rejects_decoded_traversal() {
        let root = TempRoot::new();
        fs::write(root.path().join("visible.txt"), "visible").unwrap();
        let secret = root
            .path()
            .parent()
            .unwrap()
            .join("rustee-static-secret.txt");
        fs::write(&secret, "secret").unwrap();
        let service = StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_max_file_bytes(3)
            .unwrap()
            .layer()
            .layer(fallback_service());

        assert_eq!(
            service
                .clone()
                .oneshot(request(Method::GET, "/application"))
                .await
                .unwrap()
                .status(),
            StatusCode::IM_A_TEAPOT
        );
        assert_eq!(
            service
                .clone()
                .oneshot(request(Method::POST, "/assets/visible.txt"))
                .await
                .unwrap()
                .status(),
            StatusCode::IM_A_TEAPOT
        );
        assert_eq!(
            service
                .oneshot(request(
                    Method::GET,
                    "/assets/%2e%2e/rustee-static-secret.txt"
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            StaticFiles::new(root.path())
                .unwrap()
                .at("/assets")
                .unwrap()
                .with_max_file_bytes(3)
                .unwrap()
                .layer()
                .layer(fallback_service())
                .oneshot(request(Method::GET, "/assets/visible.txt"))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let _ = fs::remove_file(secret);
    }
}
