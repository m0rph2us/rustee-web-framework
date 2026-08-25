//! HTTP request dispatch and session-expiry recovery for the MCP client.

use std::sync::atomic::Ordering;

use reqwest::{
    Response, StatusCode,
    header::{ACCEPT, CONTENT_TYPE},
};
use serde_json::{Value, json};
use tokio::time::sleep;

use super::protocol::{BoundedJsonEncodingError, McpReply, encode_bounded_json};
use super::{MCP_PROTOCOL_VERSION, McpError, McpHttpClient, McpSession};

impl McpHttpClient {
    pub(super) async fn initialize_request(&self, id: u64) -> Result<McpReply, McpError> {
        let response = self
            .post(
                None,
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "method":"initialize",
                    "params":{
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "rustee-ai-mcp",
                            "version": env!("CARGO_PKG_VERSION"),
                        },
                    },
                }),
            )
            .await?;
        self.decode_response(response, id, None).await
    }

    pub(super) async fn request(
        &self,
        session: Option<&McpSession>,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<McpReply, McpError> {
        let response = match self
            .post(
                session,
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            )
            .await
        {
            Ok(response) => response,
            Err(McpError::SessionExpired) => {
                self.recover_expired_session().await;
                return Err(McpError::SessionExpired);
            }
            Err(error) => return Err(error),
        };
        match self.decode_response(response, id, session).await {
            Err(McpError::SessionExpired) => {
                self.recover_expired_session().await;
                Err(McpError::SessionExpired)
            }
            result => result,
        }
    }

    pub(super) async fn notification(
        &self,
        session: &McpSession,
        method: &str,
        params: Value,
    ) -> Result<(), McpError> {
        let response = self
            .post(
                Some(session),
                json!({"jsonrpc":"2.0","method":method,"params":params}),
            )
            .await?;
        if response.status() == StatusCode::ACCEPTED || response.status().is_success() {
            return Ok(());
        }
        Err(McpError::HttpStatus(response.status()))
    }

    async fn post(&self, session: Option<&McpSession>, body: Value) -> Result<Response, McpError> {
        let body = encode_http_request(&body, self.config.max_request_bytes)?;
        let mut request = self
            .client
            .post(self.config.endpoint.clone())
            .timeout(self.config.request_timeout)
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(bearer_token) = &self.config.bearer_token {
            request = request.bearer_auth(bearer_token);
        }
        if let Some(session) = session {
            request = request.header("mcp-protocol-version", &session.protocol_version);
            if let Some(session_id) = &session.id {
                request = request.header("mcp-session-id", session_id.as_header_value());
            }
        }
        let response = request.send().await.map_err(|_| McpError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND
            && session.is_some_and(|session| session.id.is_some())
        {
            if let Some(session) = session {
                self.clear_expired_session(session).await;
            }
            return Err(McpError::SessionExpired);
        }
        if !response.status().is_success() {
            return Err(McpError::HttpStatus(response.status()));
        }
        Ok(response)
    }

    async fn recover_expired_session(&self) {
        let Some(recovery) = self.config.automatic_session_recovery else {
            return;
        };
        let _initializing = self.state.initialize.lock().await;
        if self.session().await.is_some() {
            return;
        }
        self.state.next_request_id.store(0, Ordering::Relaxed);
        for attempt in 0..recovery.max_attempts {
            sleep(recovery.delay_for(attempt)).await;
            if self.initialize_unlocked().await.is_ok() {
                return;
            }
        }
    }
}

fn encode_http_request(value: &Value, max_bytes: usize) -> Result<Vec<u8>, McpError> {
    encode_bounded_json(value, max_bytes).map_err(|error| match error {
        BoundedJsonEncodingError::TooLarge => McpError::RequestTooLarge,
        BoundedJsonEncodingError::Malformed => McpError::MalformedResponse,
    })
}
