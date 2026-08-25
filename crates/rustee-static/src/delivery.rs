//! Static representation resolution and file-body delivery.

use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use http::{HeaderMap, StatusCode, header::RANGE};
use rustee_core::{Body, Error, IntoResponse, Response, empty_body, full_body, stream_body};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use super::{
    StaticFiles,
    encoding::select_precompressed_variant,
    range::{ByteRange, RequestedRange, requested_range},
    response::{
        StaticResponseContext, cache_validators, content_type, is_not_modified, multipart_boundary,
        multipart_content_length, multipart_file_body, static_multipart_response,
        static_not_modified, static_range_not_satisfiable, static_success_response,
    },
};

pub(super) const STREAMING_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub(super) struct StaticRepresentation {
    pub(super) target: PathBuf,
    pub(super) metadata: std::fs::Metadata,
    pub(super) content_encoding: Option<super::encoding::PrecompressedEncoding>,
}

pub(super) async fn serve_file(
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

pub(super) fn static_not_found() -> Response {
    Error::not_found("the requested static resource was not found").into_response()
}
