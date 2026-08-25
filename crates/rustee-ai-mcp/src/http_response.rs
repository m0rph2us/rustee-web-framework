//! Bounded JSON and resumable SSE response handling for the MCP HTTP client.

use futures_util::StreamExt;
use reqwest::{
    Response, StatusCode,
    header::{ACCEPT, CONTENT_TYPE, HeaderMap},
};
use serde_json::Value;
use tokio::time::sleep;

use super::{MCP_PROTOCOL_VERSION, McpError, McpHttpClient, McpSession};
use super::{
    protocol::{MAX_SESSION_ID_BYTES, McpHeaderValue, McpReply, decode_rpc_result},
    sse::{
        SseReadOutcome, SseStreamState, parse_sse_frame, take_sse_frame, valid_sse_notification,
    },
};

impl McpHttpClient {
    pub(super) async fn decode_response(
        &self,
        response: Response,
        id: u64,
        session: Option<&McpSession>,
    ) -> Result<McpReply, McpError> {
        let session_id = response_session_id(response.headers())?;
        let value = self
            .response_value(response, id, session, session_id.as_ref())
            .await?;
        let result = decode_rpc_result(&value, id)?;
        Ok(McpReply { result, session_id })
    }

    async fn response_value(
        &self,
        response: Response,
        id: u64,
        session: Option<&McpSession>,
        response_session_id: Option<&McpHeaderValue>,
    ) -> Result<Value, McpError> {
        if content_type_is(response.headers(), "application/json") {
            return self.json_response(response).await;
        }
        if content_type_is(response.headers(), "text/event-stream") {
            let protocol_version = session.map_or(MCP_PROTOCOL_VERSION, |session| {
                session.protocol_version.as_str()
            });
            let session_id = session
                .and_then(|session| session.id.as_ref())
                .or(response_session_id);
            return self
                .sse_response(response, id, session, protocol_version, session_id)
                .await;
        }
        Err(McpError::UnsupportedResponseContentType)
    }

    async fn json_response(&self, response: Response) -> Result<Value, McpError> {
        if response
            .content_length()
            .is_some_and(|length| length > self.config.max_response_bytes as u64)
        {
            return Err(McpError::ResponseTooLarge);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| McpError::Transport)?;
            if chunk.len() > self.config.max_response_bytes.saturating_sub(body.len()) {
                return Err(McpError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| McpError::MalformedResponse)
    }

    async fn sse_response(
        &self,
        response: Response,
        expected_id: u64,
        session: Option<&McpSession>,
        protocol_version: &str,
        session_id: Option<&McpHeaderValue>,
    ) -> Result<Value, McpError> {
        let mut state = SseStreamState::default();
        let mut response = response;
        let mut attempts = 0_usize;
        loop {
            match self
                .consume_sse_response(response, expected_id, &mut state)
                .await?
            {
                SseReadOutcome::Result(value) => return Ok(value),
                SseReadOutcome::Disconnected => {}
            }
            let Some(resumption) = self.config.automatic_sse_resumption else {
                return Err(McpError::SseStreamTerminated);
            };
            let Some(last_event_id) = state.last_event_id.as_ref() else {
                return Err(McpError::SseStreamTerminated);
            };
            if attempts >= resumption.max_attempts {
                return Err(McpError::SseStreamTerminated);
            }
            let delay = state
                .retry_delay
                .unwrap_or_else(|| resumption.delay_for(attempts));
            if delay > resumption.max_backoff {
                return Err(McpError::SseRetryLimit);
            }
            sleep(delay).await;
            response = self
                .get_sse(session, protocol_version, session_id, Some(last_event_id))
                .await?;
            attempts += 1;
        }
    }

    async fn consume_sse_response(
        &self,
        response: Response,
        expected_id: u64,
        state: &mut SseStreamState,
    ) -> Result<SseReadOutcome, McpError> {
        let remaining = self
            .config
            .max_response_bytes
            .saturating_sub(state.total_bytes);
        if response
            .content_length()
            .is_some_and(|length| length > remaining as u64)
        {
            return Err(McpError::ResponseTooLarge);
        }
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                state.buffer.clear();
                return Ok(SseReadOutcome::Disconnected);
            };
            if chunk.len()
                > self
                    .config
                    .max_response_bytes
                    .saturating_sub(state.total_bytes)
            {
                return Err(McpError::ResponseTooLarge);
            }
            state.total_bytes += chunk.len();
            state.buffer.extend_from_slice(&chunk);
            while let Some(frame) = take_sse_frame(&mut state.buffer) {
                let frame = parse_sse_frame(&frame)?;
                if let Some(event_id) = frame.event_id {
                    state.last_event_id = Some(event_id);
                }
                if let Some(retry_delay) = frame.retry_delay {
                    state.retry_delay = Some(retry_delay);
                }
                let Some(payload) = frame.payload else {
                    continue;
                };
                let value = serde_json::from_str::<Value>(&payload)
                    .map_err(|_| McpError::MalformedResponse)?;
                if value.get("id").is_some() {
                    decode_rpc_result(&value, expected_id)?;
                    return Ok(SseReadOutcome::Result(value));
                }
                if !valid_sse_notification(&value) {
                    return Err(McpError::MalformedResponse);
                }
            }
            if state.buffer.len() > self.config.max_response_bytes {
                return Err(McpError::ResponseTooLarge);
            }
        }
        state.buffer.clear();
        Ok(SseReadOutcome::Disconnected)
    }

    pub(super) async fn get_sse(
        &self,
        current_session: Option<&McpSession>,
        protocol_version: &str,
        session_id: Option<&McpHeaderValue>,
        last_event_id: Option<&McpHeaderValue>,
    ) -> Result<Response, McpError> {
        let mut request = self
            .client
            .get(self.config.endpoint.clone())
            .timeout(self.config.request_timeout)
            .header(ACCEPT, "text/event-stream")
            .header("mcp-protocol-version", protocol_version);
        if let Some(last_event_id) = last_event_id {
            request = request.header("last-event-id", last_event_id.as_header_value());
        }
        if let Some(bearer_token) = &self.config.bearer_token {
            request = request.bearer_auth(bearer_token);
        }
        if let Some(session_id) = session_id {
            request = request.header("mcp-session-id", session_id.as_header_value());
        }
        let response = request.send().await.map_err(|_| McpError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND && session_id.is_some() {
            if let Some(current_session) = current_session {
                self.clear_expired_session(current_session).await;
            }
            return Err(McpError::SessionExpired);
        }
        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Err(McpError::SseStreamTerminated);
        }
        if !response.status().is_success() {
            return Err(McpError::HttpStatus(response.status()));
        }
        if !content_type_is(response.headers(), "text/event-stream") {
            return Err(McpError::UnsupportedResponseContentType);
        }
        Ok(response)
    }
}

fn content_type_is(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
}

fn response_session_id(headers: &HeaderMap) -> Result<Option<McpHeaderValue>, McpError> {
    let mut values = headers.get_all("mcp-session-id").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(McpError::MalformedResponse);
    }
    let session_id = value.to_str().map_err(|_| McpError::MalformedResponse)?;
    McpHeaderValue::from_remote(session_id, MAX_SESSION_ID_BYTES).map(Some)
}

#[cfg(test)]
mod tests {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    use super::{McpError, content_type_is, response_session_id};

    #[test]
    fn content_type_admission_requires_an_exact_media_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        assert!(content_type_is(&headers, "application/json"));

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("TEXT/EVENT-STREAM; charset=utf-8"),
        );
        assert!(content_type_is(&headers, "text/event-stream"));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/jsonp"));
        assert!(!content_type_is(&headers, "application/json"));

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream+json"),
        );
        assert!(!content_type_is(&headers, "text/event-stream"));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!content_type_is(&headers, "application/json"));

        headers.clear();
        assert!(!content_type_is(&headers, "application/json"));
    }

    #[test]
    fn response_session_id_requires_one_valid_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert("mcp-session-id", HeaderValue::from_static("session-a"));
        assert_eq!(
            response_session_id(&headers)
                .unwrap()
                .map(|value| value.as_header_value()),
            Some(HeaderValue::from_static("session-a"))
        );

        headers.append("mcp-session-id", HeaderValue::from_static("session-b"));
        assert!(matches!(
            response_session_id(&headers),
            Err(McpError::MalformedResponse)
        ));

        headers.clear();
        assert!(response_session_id(&headers).unwrap().is_none());

        headers.insert(
            "mcp-session-id",
            HeaderValue::from_static("invalid session"),
        );
        assert!(matches!(
            response_session_id(&headers),
            Err(McpError::MalformedResponse)
        ));
    }
}
