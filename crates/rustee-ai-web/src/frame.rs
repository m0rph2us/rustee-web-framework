//! Wire framing for provider-neutral AI stream events.

use std::io::{self, Write};

use bytes::Bytes;
use rustee_ai::{AiStreamEvent, Usage};
use serde::Serialize;

#[derive(Clone, Copy)]
pub(crate) enum StreamFormat {
    Sse,
    Ndjson,
}

pub(crate) fn encode_event(
    format: StreamFormat,
    event: AiStreamEvent,
    max_frame_bytes: usize,
) -> Result<Bytes, FrameEncodingError> {
    match event {
        AiStreamEvent::TextDelta(delta) => encode_frame(
            format,
            &WebEvent::TextDelta { delta: &delta },
            max_frame_bytes,
        ),
        AiStreamEvent::ToolCall(call) => encode_frame(
            format,
            &WebEvent::ToolCall {
                id: call.id(),
                name: call.name(),
                arguments: call.arguments(),
            },
            max_frame_bytes,
        ),
        AiStreamEvent::ToolResult(result) => encode_frame(
            format,
            &WebEvent::ToolResult {
                call_id: result.call_id(),
                name: result.name(),
                content: result.content(),
            },
            max_frame_bytes,
        ),
        AiStreamEvent::Completed(usage) => encode_frame(
            format,
            &WebEvent::Completed { usage: &usage },
            max_frame_bytes,
        ),
    }
}

pub(crate) fn terminal_error_frame(format: StreamFormat, error: TerminalError) -> Bytes {
    match (format, error) {
        (StreamFormat::Sse, TerminalError::StreamFailed) => {
            Bytes::from_static(b"data: {\"type\":\"error\",\"code\":\"ai_stream_failed\"}\n\n")
        }
        (StreamFormat::Sse, TerminalError::EventTooLarge) => Bytes::from_static(
            b"data: {\"type\":\"error\",\"code\":\"ai_stream_event_too_large\"}\n\n",
        ),
        (StreamFormat::Ndjson, TerminalError::StreamFailed) => {
            Bytes::from_static(b"{\"type\":\"error\",\"code\":\"ai_stream_failed\"}\n")
        }
        (StreamFormat::Ndjson, TerminalError::EventTooLarge) => {
            Bytes::from_static(b"{\"type\":\"error\",\"code\":\"ai_stream_event_too_large\"}\n")
        }
    }
}

fn encode_frame<T: Serialize>(
    format: StreamFormat,
    value: &T,
    max_frame_bytes: usize,
) -> Result<Bytes, FrameEncodingError> {
    let (prefix, suffix) = match format {
        StreamFormat::Sse => (b"data: ".as_slice(), b"\n\n".as_slice()),
        StreamFormat::Ndjson => (b"".as_slice(), b"\n".as_slice()),
    };
    let mut buffer = BoundedFrameBuffer::new(max_frame_bytes);
    if buffer.write_all(prefix).is_err() {
        return Err(FrameEncodingError::TooLarge);
    }
    let result = serde_json::to_writer(&mut buffer, value);
    if buffer.exceeded {
        return Err(FrameEncodingError::TooLarge);
    }
    result.map_err(|_| FrameEncodingError::SerializationRejected)?;
    if buffer.write_all(suffix).is_err() {
        return Err(FrameEncodingError::TooLarge);
    }
    Ok(Bytes::from(buffer.into_inner()))
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WebEvent<'a> {
    TextDelta {
        delta: &'a str,
    },
    ToolCall {
        id: &'a str,
        name: &'a str,
        arguments: &'a serde_json::Value,
    },
    ToolResult {
        call_id: &'a str,
        name: &'a str,
        content: &'a serde_json::Value,
    },
    Completed {
        usage: &'a Usage,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum FrameEncodingError {
    TooLarge,
    SerializationRejected,
}

#[derive(Clone, Copy)]
pub(crate) enum TerminalError {
    StreamFailed,
    EventTooLarge,
}

struct BoundedFrameBuffer {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedFrameBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
            exceeded: false,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedFrameBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "AI stream response frame limit exceeded",
            ));
        }

        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
