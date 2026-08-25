//! MCP Streamable HTTP SSE frame parsing and standalone notification streams.

use std::{fmt, time::Duration};

use reqwest::Response;
use serde_json::Value;
use tokio::time::sleep;

use super::protocol::McpHeaderValue;
use super::{
    MAX_SSE_EVENT_ID_BYTES, MAX_SSE_NOTIFICATION_METHOD_BYTES, McpError, McpHttpClient, McpSession,
};

/// One untrusted JSON-RPC notification received from an explicit MCP HTTP SSE `GET` stream.
///
/// Receiving this value does not invoke any tool, mutate local context, or add data to a model
/// request. The application chooses whether the declared method is meaningful and applies its
/// own authorization, validation, redaction, and bounded handling policy.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerNotification {
    method: String,
    params: Option<Value>,
}

impl McpServerNotification {
    fn from_wire(value: &Value) -> Result<Self, McpError> {
        if !valid_sse_notification(value) {
            return Err(McpError::MalformedResponse);
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| valid_sse_notification_method(method))
            .ok_or(McpError::MalformedResponse)?
            .to_owned();
        Ok(Self {
            method,
            params: value.get("params").cloned(),
        })
    }

    /// Returns the remote JSON-RPC notification method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns untrusted remote notification parameters without interpreting them.
    #[must_use]
    pub const fn params(&self) -> Option<&Value> {
        self.params.as_ref()
    }
}

impl fmt::Debug for McpServerNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerNotification")
            .field("method", &self.method)
            .field("has_params", &self.params.is_some())
            .finish()
    }
}

/// An explicit standalone MCP Streamable HTTP SSE `GET` listener.
///
/// Use [`Self::next_notification`] to read one remote notification at a time. Dropping this
/// value closes the HTTP stream without closing the MCP session. Server-initiated requests and
/// responses are rejected instead of being executed or answered automatically.
/// The configured response-byte limit applies cumulatively across this stream and any automatic
/// resumptions.
pub struct McpServerEventStream {
    client: McpHttpClient,
    session: McpSession,
    response: Option<Response>,
    buffer: Vec<u8>,
    total_bytes: usize,
    last_event_id: Option<McpHeaderValue>,
    retry_delay: Option<Duration>,
    resume_attempts: usize,
    closed: bool,
}

impl McpServerEventStream {
    pub(super) fn new(
        client: McpHttpClient,
        session: McpSession,
        response: Response,
    ) -> Result<Self, McpError> {
        if response
            .content_length()
            .is_some_and(|length| length > client.config.max_response_bytes as u64)
        {
            return Err(McpError::ResponseTooLarge);
        }
        Ok(Self {
            client,
            session,
            response: Some(response),
            buffer: Vec::new(),
            total_bytes: 0,
            last_event_id: None,
            retry_delay: None,
            resume_attempts: 0,
            closed: false,
        })
    }

    /// Reads the next standalone server notification.
    ///
    /// A normal connection close returns [`McpError::SseStreamTerminated`] unless the stream
    /// supplied an event ID and the client explicitly enabled bounded SSE resumption. Resumption
    /// uses only `GET` plus `Last-Event-ID`; it never emits a JSON-RPC `POST`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized protocol, transport, session, byte-limit, or termination failure.
    pub async fn next_notification(&mut self) -> Result<McpServerNotification, McpError> {
        if self.closed {
            return Err(McpError::SseStreamTerminated);
        }
        loop {
            if let Some(frame) = take_sse_frame(&mut self.buffer) {
                let frame = match parse_sse_frame(&frame) {
                    Ok(frame) => frame,
                    Err(error) => return Err(self.close_with(error)),
                };
                if let Some(event_id) = frame.event_id {
                    self.last_event_id = Some(event_id);
                }
                if let Some(retry_delay) = frame.retry_delay {
                    self.retry_delay = Some(retry_delay);
                }
                let Some(payload) = frame.payload else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(&payload) else {
                    return Err(self.close_with(McpError::MalformedResponse));
                };
                return McpServerNotification::from_wire(&value)
                    .map_err(|error| self.close_with(error));
            }

            if self.buffer.len() > self.client.config.max_response_bytes {
                return Err(self.close_with(McpError::ResponseTooLarge));
            }
            let next_chunk = match self.response.as_mut() {
                Some(response) => response.chunk().await,
                None => return Err(self.close_with(McpError::SseStreamTerminated)),
            };
            match next_chunk {
                Ok(Some(chunk)) => {
                    if chunk.len()
                        > self
                            .client
                            .config
                            .max_response_bytes
                            .saturating_sub(self.total_bytes)
                    {
                        return Err(self.close_with(McpError::ResponseTooLarge));
                    }
                    self.total_bytes += chunk.len();
                    self.buffer.extend_from_slice(&chunk);
                }
                Ok(None) | Err(_) => self.resume_after_disconnect().await?,
            }
        }
    }

    /// Stops reading this SSE connection without closing the MCP session.
    pub fn close(mut self) {
        self.closed = true;
        self.response = None;
        self.buffer.clear();
    }

    fn close_with(&mut self, error: McpError) -> McpError {
        self.closed = true;
        self.response = None;
        self.buffer.clear();
        error
    }

    async fn resume_after_disconnect(&mut self) -> Result<(), McpError> {
        self.response = None;
        self.buffer.clear();
        let Some(resumption) = self.client.config.automatic_sse_resumption else {
            return Err(self.close_with(McpError::SseStreamTerminated));
        };
        let Some(last_event_id) = self.last_event_id.clone() else {
            return Err(self.close_with(McpError::SseStreamTerminated));
        };
        if self.resume_attempts >= resumption.max_attempts {
            return Err(self.close_with(McpError::SseStreamTerminated));
        }
        let delay = self
            .retry_delay
            .unwrap_or_else(|| resumption.delay_for(self.resume_attempts));
        if delay > resumption.max_backoff {
            return Err(self.close_with(McpError::SseRetryLimit));
        }
        sleep(delay).await;
        let client = self.client.clone();
        let session = self.session.clone();
        let response = client
            .get_sse(
                Some(&session),
                &session.protocol_version,
                session.id.as_ref(),
                Some(&last_event_id),
            )
            .await;
        match response {
            Ok(response) => {
                let remaining = self
                    .client
                    .config
                    .max_response_bytes
                    .saturating_sub(self.total_bytes);
                if response
                    .content_length()
                    .is_some_and(|length| length > remaining as u64)
                {
                    return Err(self.close_with(McpError::ResponseTooLarge));
                }
                self.response = Some(response);
                self.resume_attempts += 1;
                Ok(())
            }
            Err(error) => Err(self.close_with(error)),
        }
    }
}

impl fmt::Debug for McpServerEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerEventStream")
            .field("has_event_id", &self.last_event_id.is_some())
            .field("resume_attempts", &self.resume_attempts)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
#[derive(Default)]
pub(super) struct SseStreamState {
    pub(super) total_bytes: usize,
    pub(super) buffer: Vec<u8>,
    pub(super) last_event_id: Option<McpHeaderValue>,
    pub(super) retry_delay: Option<Duration>,
}

pub(super) enum SseReadOutcome {
    Result(Value),
    Disconnected,
}

pub(super) struct SseFrame {
    pub(super) event_id: Option<McpHeaderValue>,
    pub(super) retry_delay: Option<Duration>,
    pub(super) payload: Option<String>,
}

pub(super) fn take_sse_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let delimiter = [(b"\r\n\r\n".as_slice(), 4), (b"\n\n", 2), (b"\r\r", 2)]
        .into_iter()
        .filter_map(|(needle, length)| {
            buffer
                .windows(needle.len())
                .position(|window| window == needle)
                .map(|index| (index, length))
        })
        .min_by_key(|(index, _)| *index)?;
    Some(buffer.drain(..delimiter.0 + delimiter.1).collect())
}

#[cfg(test)]
pub(super) fn sse_payload(frame: &[u8]) -> Result<Option<String>, McpError> {
    Ok(parse_sse_frame(frame)?.payload)
}

pub(super) fn parse_sse_frame(frame: &[u8]) -> Result<SseFrame, McpError> {
    let frame = std::str::from_utf8(frame)
        .map_err(|_| McpError::MalformedResponse)?
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let mut event_id = None;
    let mut retry_delay = None;
    let mut payload = Vec::new();
    for line in frame.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => payload.push(value),
            "id" => {
                event_id = Some(McpHeaderValue::from_remote(value, MAX_SSE_EVENT_ID_BYTES)?);
            }
            "retry" if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                retry_delay = value.parse::<u64>().ok().map(Duration::from_millis);
            }
            _ => {}
        }
    }
    let payload = (!payload.is_empty())
        .then(|| payload.join("\n"))
        .filter(|payload| !payload.is_empty());
    Ok(SseFrame {
        event_id,
        retry_delay,
        payload,
    })
}

pub(super) fn valid_sse_notification(value: &Value) -> bool {
    value.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && value.get("method").and_then(Value::as_str).is_some()
        && value.get("id").is_none()
        && value.get("result").is_none()
        && value.get("error").is_none()
}

pub(super) fn valid_sse_notification_method(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SSE_NOTIFICATION_METHOD_BYTES
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}
