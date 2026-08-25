//! Bounded SSE frame buffering and `OpenAI` stream-event decoding.

use rustee_ai::{AiStreamEvent, ToolCall};
use serde_json::Value;

use crate::OpenAiError;

use super::usage;

pub(crate) fn decode_stream_event(payload: &str) -> Result<Option<AiStreamEvent>, OpenAiError> {
    let value: Value =
        serde_json::from_str(payload).map_err(|_| OpenAiError::MalformedStreamEvent)?;
    match value.get("type").and_then(Value::as_str) {
        Some("response.output_text.delta") => value
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| Some(AiStreamEvent::TextDelta(delta.to_owned())))
            .ok_or(OpenAiError::MalformedStreamEvent),
        Some("response.function_call_arguments.done") => {
            let call_id = value
                .get("call_id")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let arguments = value
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let arguments =
                serde_json::from_str(arguments).map_err(|_| OpenAiError::MalformedStreamEvent)?;
            let call = ToolCall::new(call_id, name, arguments)
                .map_err(|_| OpenAiError::MalformedStreamEvent)?;
            Ok(Some(AiStreamEvent::ToolCall(call)))
        }
        Some("response.completed") => Ok(Some(AiStreamEvent::Completed(usage(
            value
                .get("response")
                .and_then(|response| response.get("usage")),
        )?))),
        Some("response.failed" | "error") => Err(OpenAiError::MalformedStreamEvent),
        _ => Ok(None),
    }
}

#[derive(Default)]
pub(crate) struct SseFrameBuffer {
    bytes: Vec<u8>,
    start: usize,
}

impl SseFrameBuffer {
    fn len(&self) -> usize {
        self.bytes.len().saturating_sub(self.start)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn reclaim_consumed_prefix(&mut self) {
        if self.start == self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        } else if self.start >= self.bytes.len().saturating_sub(self.start) {
            self.bytes.drain(..self.start);
            self.start = 0;
        }
    }

    fn take_frame(&mut self) -> Option<Vec<u8>> {
        let bytes = &self.bytes[self.start..];
        let end = self.start + sse_frame_end(bytes)?;
        let frame = self.bytes[self.start..end].to_vec();
        self.start = end;
        self.reclaim_consumed_prefix();
        Some(frame)
    }
}

pub(crate) fn take_sse_frame(buffer: &mut SseFrameBuffer) -> Option<Vec<u8>> {
    buffer.take_frame()
}

pub(crate) fn append_sse_chunk(
    buffer: &mut SseFrameBuffer,
    chunk: &[u8],
    max_sse_event_bytes: usize,
) -> Result<(), OpenAiError> {
    if chunk.len() > max_sse_event_bytes.saturating_sub(buffer.len()) {
        return Err(OpenAiError::StreamEventTooLarge);
    }
    buffer.reclaim_consumed_prefix();
    buffer.bytes.extend_from_slice(chunk);
    Ok(())
}

pub(crate) fn sse_payload(frame: &[u8]) -> Result<String, OpenAiError> {
    let frame = std::str::from_utf8(frame).map_err(|_| OpenAiError::MalformedStreamEvent)?;
    let payload = frame
        .split(['\r', '\n'])
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|line| line.strip_prefix(' ').unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    (!payload.is_empty())
        .then_some(payload)
        .ok_or(OpenAiError::MalformedStreamEvent)
}

/// Finds one complete SSE event, accepting every valid field-line terminator.
fn sse_frame_end(bytes: &[u8]) -> Option<usize> {
    let mut index = 0;
    let mut line_start = 0;
    while index < bytes.len() {
        let line_break_width = match bytes[index] {
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => 2,
            b'\r' | b'\n' => 1,
            _ => {
                index += 1;
                continue;
            }
        };
        if index == line_start {
            return Some(index + line_break_width);
        }
        index += line_break_width;
        line_start = index;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{SseFrameBuffer, append_sse_chunk, sse_payload, take_sse_frame};

    #[test]
    fn sse_frames_accept_cr_lf_and_mixed_line_endings() {
        let mut buffer = SseFrameBuffer::default();
        append_sse_chunk(
            &mut buffer,
            b"data: first\rdata: second\r\rdata: third\r\n\ndata: fourth\n\r\n",
            256,
        )
        .unwrap();

        let payloads = std::iter::from_fn(|| take_sse_frame(&mut buffer))
            .map(|frame| sse_payload(&frame).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(payloads, ["first\nsecond", "third", "fourth"]);
        assert!(buffer.is_empty());
    }
}
