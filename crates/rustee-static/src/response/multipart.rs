//! Multipart byte-range framing, length accounting, and file streaming.

use std::{
    io::{self, SeekFrom},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::StreamExt;
use http::HeaderValue;
use rustee_core::{Body, stream_body};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use super::super::range::ByteRange;

const STREAMING_CHUNK_BYTES: usize = 16 * 1024;
static NEXT_MULTIPART_BOUNDARY: AtomicU64 = AtomicU64::new(0);

pub(crate) fn multipart_boundary() -> String {
    let counter = NEXT_MULTIPART_BOUNDARY.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("rustee-{timestamp:x}-{counter:x}")
}

pub(crate) fn multipart_content_length(
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

pub(crate) fn multipart_file_body(
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
