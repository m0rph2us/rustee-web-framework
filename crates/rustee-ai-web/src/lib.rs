//! HTTP streaming responses for Rustee AI events.
//!
//! The adapter turns a provider-neutral [`rustee_ai::AiEventStream`] into SSE or NDJSON while
//! keeping upstream error details out of the browser response. Dropping the response body drops
//! the upstream stream and therefore participates in normal transport cancellation.

use std::{convert::Infallible, error::Error as StdError};

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_ai::AiEventStream;
use rustee_core::{Response, response, stream_body};

mod frame;

use frame::{FrameEncodingError, StreamFormat, TerminalError, encode_event, terminal_error_frame};

/// Default upper limit for one complete encoded SSE or NDJSON event frame.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

const MIN_MAX_FRAME_BYTES: usize = 128;

/// Validated byte limit for one AI web streaming event frame.
///
/// The limit includes JSON encoding and the SSE or NDJSON framing bytes. The default supports
/// ordinary model deltas while keeping a single unexpected event from becoming an unbounded
/// response allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AiStreamResponseConfig {
    max_frame_bytes: usize,
}

impl AiStreamResponseConfig {
    /// Creates the default streaming response configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }

    /// Sets the maximum encoded bytes for one complete SSE or NDJSON event frame.
    ///
    /// # Errors
    ///
    /// Returns [`AiStreamResponseConfigError::FrameLimitTooSmall`] when the limit cannot encode
    /// the adapter's terminal error frame.
    pub fn with_max_frame_bytes(
        mut self,
        max_frame_bytes: usize,
    ) -> Result<Self, AiStreamResponseConfigError> {
        if max_frame_bytes < MIN_MAX_FRAME_BYTES {
            return Err(AiStreamResponseConfigError::FrameLimitTooSmall);
        }
        self.max_frame_bytes = max_frame_bytes;
        Ok(self)
    }

    /// Returns the maximum encoded bytes for one complete SSE or NDJSON event frame.
    #[must_use]
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }
}

impl Default for AiStreamResponseConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid AI web streaming response configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiStreamResponseConfigError {
    /// The configured frame limit cannot carry a generic terminal error frame.
    #[error("AI stream response frame limit must be at least 128 bytes")]
    FrameLimitTooSmall,
}

/// Creates a `text/event-stream` response from a provider-neutral AI stream.
///
/// Every event is encoded as one JSON SSE data frame. Terminal completion or an upstream failure
/// stops polling the upstream stream. Failures become one generic `ai_stream_failed` event;
/// provider error text is never sent to the client.
#[must_use]
pub fn sse<E>(stream: AiEventStream<E>) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    sse_with_config(stream, AiStreamResponseConfig::default())
}

/// Creates an SSE response with an explicit per-frame byte limit.
///
/// Terminal completion stops polling the upstream stream. An upstream failure or oversized event
/// emits one generic terminal error frame without provider details, then stops polling it too.
#[must_use]
pub fn sse_with_config<E>(stream: AiEventStream<E>, config: AiStreamResponseConfig) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    stream_response(stream, StreamFormat::Sse, config)
}

/// Creates an `application/x-ndjson` response from a provider-neutral AI stream.
///
/// Every event is encoded as one newline-delimited JSON object. Terminal completion stops polling
/// the upstream stream. Provider error details are normalized to a generic error object.
#[must_use]
pub fn ndjson<E>(stream: AiEventStream<E>) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    ndjson_with_config(stream, AiStreamResponseConfig::default())
}

/// Creates an NDJSON response with an explicit per-frame byte limit.
///
/// Terminal completion stops polling the upstream stream. An upstream failure or oversized event
/// emits one generic terminal error frame without provider details, then stops polling it too.
#[must_use]
pub fn ndjson_with_config<E>(stream: AiEventStream<E>, config: AiStreamResponseConfig) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    stream_response(stream, StreamFormat::Ndjson, config)
}

fn stream_response<E>(
    stream: AiEventStream<E>,
    format: StreamFormat,
    config: AiStreamResponseConfig,
) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    let stream = async_stream::stream! {
        for await event in stream {
            let (frame, terminal) = match event {
                Ok(event) => {
                    let completed = matches!(&event, rustee_ai::AiStreamEvent::Completed(_));
                    match encode_event(format, event, config.max_frame_bytes()) {
                        Ok(frame) => (frame, completed),
                        Err(FrameEncodingError::TooLarge) => {
                            (terminal_error_frame(format, TerminalError::EventTooLarge), true)
                        }
                        Err(FrameEncodingError::SerializationRejected) => {
                            (terminal_error_frame(format, TerminalError::StreamFailed), true)
                        }
                    }
                }
                Err(_) => (terminal_error_frame(format, TerminalError::StreamFailed), true),
            };
            yield Ok::<_, Infallible>(frame);
            if terminal {
                break;
            }
        };
    };
    let mut response = response(StatusCode::OK, stream_body(stream));
    let headers = response.headers_mut();
    match format {
        StreamFormat::Sse => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            );
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-cache, no-transform"),
            );
            headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
        }
        StreamFormat::Ndjson => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson; charset=utf-8"),
            );
            headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        }
    }
    response
}

#[cfg(test)]
mod tests;
