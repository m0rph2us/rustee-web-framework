//! Local subprocess connection ownership and bounded stdio JSON-RPC framing.

use std::{process::Stdio, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

use super::McpStdioConfig;
use crate::context::{McpServerCapabilities, parse_server_capabilities};
use crate::protocol::{BoundedJsonEncodingError, decode_rpc_result, encode_bounded_json};
use crate::{MCP_PROTOCOL_VERSION, McpError};

/// One initialized or initializing trusted local MCP subprocess connection.
pub(super) struct StdioConnection {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    pub(super) capabilities: McpServerCapabilities,
}

impl StdioConnection {
    pub(super) fn spawn(config: &McpStdioConfig) -> Result<Self, McpError> {
        let mut child = Command::new(&config.program)
            .args(&config.arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| McpError::StdioSpawn)?;
        Ok(Self {
            stdin: Some(child.stdin.take().ok_or(McpError::StdioSpawn)?),
            stdout: BufReader::new(child.stdout.take().ok_or(McpError::StdioSpawn)?),
            capabilities: McpServerCapabilities::default(),
            child,
        })
    }

    pub(super) async fn shutdown(&mut self, shutdown_timeout: Duration) -> Result<(), McpError> {
        let stdin_result = match self.stdin.take() {
            Some(mut stdin) => stdin.shutdown().await.map_err(|_| McpError::Transport),
            None => Ok(()),
        };
        match timeout(shutdown_timeout, self.child.wait()).await {
            Ok(result) => {
                result.map_err(|_| McpError::Transport)?;
            }
            Err(_) => {
                if self
                    .child
                    .try_wait()
                    .map_err(|_| McpError::Transport)?
                    .is_none()
                {
                    self.child.start_kill().map_err(|_| McpError::Transport)?;
                    timeout(shutdown_timeout, self.child.wait())
                        .await
                        .map_err(|_| McpError::StdioShutdownTimeout)?
                        .map_err(|_| McpError::Transport)?;
                }
            }
        }
        stdin_result
    }

    pub(super) async fn notification(
        &mut self,
        config: &McpStdioConfig,
        method: &str,
        params: Value,
    ) -> Result<(), McpError> {
        timeout(
            config.request_timeout,
            self.write_message(
                json!({"jsonrpc":"2.0","method":method,"params":params}),
                config.max_message_bytes,
            ),
        )
        .await
        .map_err(|_| McpError::StdioTimeout)?
    }

    pub(super) async fn request(
        &mut self,
        config: &McpStdioConfig,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, McpError> {
        timeout(
            config.request_timeout,
            self.request_with_deadline(
                id,
                method,
                params,
                config.max_message_bytes,
                config.max_interleaved_messages,
            ),
        )
        .await
        .map_err(|_| McpError::StdioTimeout)?
    }

    pub(super) fn accept_initialize_response(&mut self, result: &Value) -> Result<(), McpError> {
        self.capabilities = validate_initialize(result)?;
        Ok(())
    }

    async fn request_with_deadline(
        &mut self,
        id: u64,
        method: &str,
        params: Value,
        max_message_bytes: usize,
        max_interleaved_messages: usize,
    ) -> Result<Value, McpError> {
        self.write_message(
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            max_message_bytes,
        )
        .await?;
        for _ in 0..max_interleaved_messages {
            let line = read_line(&mut self.stdout, max_message_bytes)
                .await?
                .ok_or(McpError::StdioTerminated)?;
            if line.is_empty() {
                continue;
            }
            if let Some(result) = decode_stdio_message(&line, id)? {
                return Ok(result);
            }
        }
        Err(McpError::StdioMessageLimit)
    }

    async fn write_message(
        &mut self,
        value: Value,
        max_message_bytes: usize,
    ) -> Result<(), McpError> {
        let encoded = encode_message(&value, max_message_bytes)?;
        let stdin = self.stdin.as_mut().ok_or(McpError::Transport)?;
        stdin
            .write_all(&encoded)
            .await
            .map_err(|_| McpError::Transport)?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|_| McpError::Transport)?;
        stdin.flush().await.map_err(|_| McpError::Transport)
    }
}

pub(super) fn decode_stdio_message(line: &[u8], id: u64) -> Result<Option<Value>, McpError> {
    let value = serde_json::from_slice::<Value>(line).map_err(|_| McpError::MalformedResponse)?;
    if value.get("id").is_some() {
        return decode_rpc_result(&value, id).map(Some);
    }
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("method").and_then(Value::as_str).is_none()
    {
        return Err(McpError::MalformedResponse);
    }
    Ok(None)
}

fn validate_initialize(result: &Value) -> Result<McpServerCapabilities, McpError> {
    let server = result
        .get("serverInfo")
        .and_then(Value::as_object)
        .ok_or(McpError::MalformedResponse)?;
    let capabilities = parse_server_capabilities(
        result
            .get("capabilities")
            .ok_or(McpError::MalformedResponse)?,
    )?;
    if result.get("protocolVersion").and_then(Value::as_str) != Some(MCP_PROTOCOL_VERSION)
        || server
            .get("name")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        || server
            .get("version")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(McpError::MalformedResponse);
    }
    Ok(capabilities)
}

async fn read_line(
    reader: &mut BufReader<ChildStdout>,
    limit: usize,
) -> Result<Option<Vec<u8>>, McpError> {
    let mut line = Vec::new();
    loop {
        let (take, complete, bytes) = {
            let buffer = reader.fill_buf().await.map_err(|_| McpError::Transport)?;
            if buffer.is_empty() {
                return if line.is_empty() {
                    Ok(None)
                } else {
                    Err(McpError::StdioTerminated)
                };
            }
            let end = buffer
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(buffer.len(), |index| index + 1);
            (
                end,
                end < buffer.len() || buffer[end - 1] == b'\n',
                buffer[..end].to_vec(),
            )
        };
        if line.len().saturating_add(bytes.len()) > limit {
            return Err(McpError::ResponseTooLarge);
        }
        line.extend_from_slice(&bytes);
        reader.consume(take);
        if complete {
            break;
        }
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    Ok(Some(line))
}

pub(super) fn encode_message(value: &Value, max_bytes: usize) -> Result<Vec<u8>, McpError> {
    encode_bounded_json(value, max_bytes).map_err(|error| match error {
        BoundedJsonEncodingError::TooLarge => McpError::StdioRequestTooLarge,
        BoundedJsonEncodingError::Malformed => McpError::MalformedResponse,
    })
}
