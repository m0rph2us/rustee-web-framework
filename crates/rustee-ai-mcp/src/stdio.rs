use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::{sync::Mutex, time::sleep};

use super::{MCP_PROTOCOL_VERSION, McpError, McpToolDefinition};
use crate::protocol::{decode_tool_result, valid_cursor};

mod config;
mod connection;
mod context;
mod tool;

use connection::StdioConnection;

pub use config::{
    MAX_STDIO_ARGUMENT_BYTES, MAX_STDIO_ARGUMENT_COUNT, McpStdioConfig, McpStdioConfigError,
};
pub use tool::McpStdioRemoteTool;

#[cfg(feature = "fuzzing")]
pub(super) fn fuzz_decode_stdio_message(line: &[u8]) {
    let _ = connection::decode_stdio_message(line, 1);
}

#[derive(Default)]
struct StdioState {
    connection: Mutex<Option<StdioConnection>>,
    initialize: Mutex<()>,
    next_request_id: AtomicU64,
}

/// Cloneable, serialized stdio MCP client for one application-trusted subprocess.
#[derive(Clone)]
pub struct McpStdioClient {
    config: McpStdioConfig,
    state: Arc<StdioState>,
}

impl McpStdioClient {
    /// Creates an uninitialized client for one application-trusted subprocess configuration.
    #[must_use]
    pub fn new(config: McpStdioConfig) -> Self {
        Self {
            config,
            state: Arc::new(StdioState::default()),
        }
    }

    /// Starts the subprocess, performs MCP initialization, and sends `notifications/initialized`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized spawn, transport, bounds, or protocol failure.
    pub async fn initialize(&self) -> Result<(), McpError> {
        let _initializing = self.state.initialize.lock().await;
        if self.state.connection.lock().await.is_some() {
            return Ok(());
        }
        let mut connection = StdioConnection::spawn(&self.config)?;
        self.initialize_connection(&mut connection).await?;
        *self.state.connection.lock().await = Some(connection);
        Ok(())
    }

    /// Explicitly replaces the local subprocess and completes a new MCP handshake.
    ///
    /// This method closes the current stdin, waits for its bounded graceful exit, and terminates
    /// it only when necessary before starting the replacement process. It never replays a prior
    /// discovery or `tools/call`; callers decide whether a later action is safe to issue again.
    ///
    /// # Errors
    ///
    /// Returns a sanitized shutdown, spawn, transport, bounds, or protocol failure. A failed
    /// restart leaves this client uninitialized.
    pub async fn restart(&self) -> Result<(), McpError> {
        let _initializing = self.state.initialize.lock().await;
        let previous = self.state.connection.lock().await.take();
        if let Some(mut previous) = previous {
            previous.shutdown(self.config.shutdown_timeout).await?;
        }
        self.state.next_request_id.store(0, Ordering::Relaxed);
        let mut connection = StdioConnection::spawn(&self.config)?;
        self.initialize_connection(&mut connection).await?;
        *self.state.connection.lock().await = Some(connection);
        Ok(())
    }

    /// Discovers bounded remote tools after successful initialization.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`] or a sanitized transport/protocol failure.
    pub async fn list_tools(&self) -> Result<Vec<McpToolDefinition>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_tools_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_tools_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpToolDefinition>, McpError> {
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut tools = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let mut params = serde_json::Map::new();
            if let Some(cursor) = &cursor {
                params.insert("cursor".to_owned(), Value::String(cursor.clone()));
            }
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "tools/list",
                    Value::Object(params),
                )
                .await?;
            let values = result
                .get("tools")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in values {
                let tool =
                    McpToolDefinition::from_wire(value).map_err(|_| McpError::MalformedResponse)?;
                if !names.insert(tool.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                tools.push(tool);
                if tools.len() > self.config.max_tools {
                    return Err(McpError::ToolDiscoveryLimit);
                }
            }
            let next = match result.get("nextCursor") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) if valid_cursor(value) => Some(value.clone()),
                Some(_) => return Err(McpError::MalformedResponse),
            };
            let Some(next) = next else {
                return Ok(tools);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ToolDiscoveryLimit)
    }

    async fn call_tool(
        &self,
        name: String,
        arguments: Value,
        idempotency_key: Option<String>,
    ) -> Result<Value, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => {
                self.call_tool_on(connection, name, arguments, idempotency_key)
                    .await
            }
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn call_tool_on(
        &self,
        connection: &mut StdioConnection,
        name: String,
        arguments: Value,
        idempotency_key: Option<String>,
    ) -> Result<Value, McpError> {
        let mut params = serde_json::Map::new();
        params.insert("name".to_owned(), Value::String(name));
        params.insert("arguments".to_owned(), arguments);
        if let Some(key) = idempotency_key {
            params.insert("_meta".to_owned(), json!({"io.rustee/idempotency-key":key}));
        }
        let result = self
            .request_on(
                connection,
                self.next_request_id(),
                "tools/call",
                Value::Object(params),
            )
            .await?;
        decode_tool_result(&result)
    }

    /// Closes stdin, waits for a bounded graceful exit, then terminates the subprocess if needed.
    ///
    /// # Errors
    ///
    /// Returns a sanitized subprocess shutdown failure.
    pub async fn close(&self) -> Result<(), McpError> {
        let _initializing = self.state.initialize.lock().await;
        let Some(mut connection) = self.state.connection.lock().await.take() else {
            return Ok(());
        };
        connection.shutdown(self.config.shutdown_timeout).await
    }

    fn next_request_id(&self) -> u64 {
        self.state.next_request_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    async fn initialize_connection(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<(), McpError> {
        let id = self.next_request_id();
        let result = self
            .request_on(
                connection,
                id,
                "initialize",
                json!({"protocolVersion":MCP_PROTOCOL_VERSION,"capabilities":{},"clientInfo":{"name":"rustee-ai-mcp","version":env!("CARGO_PKG_VERSION")}}),
            )
            .await?;
        connection.accept_initialize_response(&result)?;
        connection
            .notification(&self.config, "notifications/initialized", json!({}))
            .await
    }

    async fn request_on(
        &self,
        connection: &mut StdioConnection,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, McpError> {
        connection.request(&self.config, id, method, params).await
    }

    async fn finish_request<T>(
        &self,
        result: Result<T, McpError>,
        stale: Option<StdioConnection>,
    ) -> Result<T, McpError> {
        let Some(mut stale) = stale else {
            return result;
        };
        if stale.shutdown(self.config.shutdown_timeout).await.is_ok() {
            self.recover_after_discard().await;
        }
        result
    }

    async fn recover_after_discard(&self) {
        let Some(restart) = self.config.automatic_restart else {
            return;
        };
        let _initializing = self.state.initialize.lock().await;
        if self.state.connection.lock().await.is_some() {
            return;
        }
        self.state.next_request_id.store(0, Ordering::Relaxed);
        for attempt in 0..restart.max_attempts {
            sleep(restart.delay_for(attempt)).await;
            let Ok(mut connection) = StdioConnection::spawn(&self.config) else {
                continue;
            };
            if self.initialize_connection(&mut connection).await.is_ok() {
                *self.state.connection.lock().await = Some(connection);
                return;
            }
            let _ = connection.shutdown(self.config.shutdown_timeout).await;
        }
    }
}

fn should_discard_connection<T>(result: &Result<T, McpError>) -> bool {
    matches!(
        result,
        Err(McpError::Transport
            | McpError::ResponseTooLarge
            | McpError::StdioMessageLimit
            | McpError::StdioTerminated
            | McpError::StdioTimeout
            | McpError::MalformedResponse)
    )
}

impl fmt::Debug for McpStdioClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpStdioClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "stdio/tests.rs"]
mod tests;
