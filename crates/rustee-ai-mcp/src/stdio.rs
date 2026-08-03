use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fmt,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::future::BoxFuture;
use rustee_ai::{ToolDefinition, ToolExecutionContext, ToolExecutionError, ToolExecutor, ToolRisk};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::{sleep, timeout},
};

use super::{
    MCP_PROTOCOL_VERSION, McpError, McpPrompt, McpPromptResult, McpResource, McpResourceContents,
    McpResourceTemplate, McpToolDefinition, decode_rpc_result, decode_tool_result, next_cursor,
    paginated_params, valid_context_name, valid_context_request_string, valid_cursor,
};
use crate::context::{
    McpServerCapabilities, parse_prompt, parse_prompt_result, parse_resource,
    parse_resource_contents, parse_resource_template, parse_server_capabilities,
};

const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_LIST_PAGES: usize = 16;
const DEFAULT_MAX_TOOLS: usize = 128;
const DEFAULT_MAX_INTERLEAVED_MESSAGES: usize = 32;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
const DEFAULT_MAX_CONTEXT_BYTES: usize = 512 * 1024;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_AUTOMATIC_RESTART_ATTEMPTS: usize = 8;
const MAX_AUTOMATIC_RESTART_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
struct StdioAutomaticRestart {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl StdioAutomaticRestart {
    fn delay_for(self, attempt: usize) -> Duration {
        let mut delay = self.initial_backoff;
        for _ in 0..attempt {
            delay = delay.saturating_mul(2).min(self.max_backoff);
        }
        delay
    }
}

/// Explicit local subprocess configuration for one stdio MCP server.
#[derive(Clone)]
pub struct McpStdioConfig {
    program: PathBuf,
    arguments: Vec<OsString>,
    max_message_bytes: usize,
    max_list_pages: usize,
    max_tools: usize,
    max_interleaved_messages: usize,
    max_context_items: usize,
    max_context_bytes: usize,
    request_timeout: Duration,
    shutdown_timeout: Duration,
    automatic_restart: Option<StdioAutomaticRestart>,
}

impl McpStdioConfig {
    /// Creates a local command configuration. The application owns command and inherited-env trust.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::BlankProgram`] when the program is empty.
    pub fn new(program: impl Into<PathBuf>) -> Result<Self, McpStdioConfigError> {
        let program = program.into();
        if program.as_os_str().is_empty() {
            return Err(McpStdioConfigError::BlankProgram);
        }
        Ok(Self {
            program,
            arguments: Vec::new(),
            max_message_bytes: DEFAULT_MAX_BYTES,
            max_list_pages: DEFAULT_MAX_LIST_PAGES,
            max_tools: DEFAULT_MAX_TOOLS,
            max_interleaved_messages: DEFAULT_MAX_INTERLEAVED_MESSAGES,
            max_context_items: DEFAULT_MAX_CONTEXT_ITEMS,
            max_context_bytes: DEFAULT_MAX_CONTEXT_BYTES,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            automatic_restart: None,
        })
    }

    /// Adds exact local command arguments. Debug output never includes their contents.
    #[must_use]
    pub fn with_arguments<Arguments, Argument>(mut self, arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<OsString>,
    {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    /// Sets one shared bound for encoded requests and stdout message lines.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ZeroMessageLimit`] when `bytes` is zero.
    pub fn with_max_message_bytes(mut self, bytes: usize) -> Result<Self, McpStdioConfigError> {
        if bytes == 0 {
            return Err(McpStdioConfigError::ZeroMessageLimit);
        }
        self.max_message_bytes = bytes;
        Ok(self)
    }

    /// Sets discovery and pre-result notification bounds.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ZeroLimit`] when any limit is zero.
    pub fn with_limits(
        mut self,
        max_list_pages: usize,
        max_tools: usize,
        max_interleaved_messages: usize,
    ) -> Result<Self, McpStdioConfigError> {
        if max_list_pages == 0 || max_tools == 0 || max_interleaved_messages == 0 {
            return Err(McpStdioConfigError::ZeroLimit);
        }
        self.max_list_pages = max_list_pages;
        self.max_tools = max_tools;
        self.max_interleaved_messages = max_interleaved_messages;
        Ok(self)
    }

    /// Sets total item and decoded-content bounds for MCP resources and prompts.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ZeroContextLimit`] when either limit is zero.
    pub fn with_context_limits(
        mut self,
        max_context_items: usize,
        max_context_bytes: usize,
    ) -> Result<Self, McpStdioConfigError> {
        if max_context_items == 0 || max_context_bytes == 0 {
            return Err(McpStdioConfigError::ZeroContextLimit);
        }
        self.max_context_items = max_context_items;
        self.max_context_bytes = max_context_bytes;
        Ok(self)
    }

    /// Sets a finite deadline for one stdin write and matching stdout response.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ZeroRequestTimeout`] when `request_timeout` is zero.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, McpStdioConfigError> {
        if request_timeout.is_zero() {
            return Err(McpStdioConfigError::ZeroRequestTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Sets the bounded grace period after stdin closes before the subprocess is killed.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ZeroShutdownTimeout`] when `shutdown_timeout` is zero.
    pub fn with_shutdown_timeout(
        mut self,
        shutdown_timeout: Duration,
    ) -> Result<Self, McpStdioConfigError> {
        if shutdown_timeout.is_zero() {
            return Err(McpStdioConfigError::ZeroShutdownTimeout);
        }
        self.shutdown_timeout = shutdown_timeout;
        Ok(self)
    }

    /// Enables bounded automatic replacement after a discarded stdio connection.
    ///
    /// A failed discovery, read, prompt get, or tool call always returns its original error. This
    /// option only prepares a freshly initialized subprocess for a later explicit request; it
    /// never replays the failed request or tool call.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for zero or excessive attempts, zero delays, or a maximum
    /// backoff below the initial backoff.
    pub fn with_automatic_restart(
        mut self,
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, McpStdioConfigError> {
        if max_attempts == 0 {
            return Err(McpStdioConfigError::ZeroRestartAttempts);
        }
        if max_attempts > MAX_AUTOMATIC_RESTART_ATTEMPTS {
            return Err(McpStdioConfigError::RestartAttemptLimit);
        }
        if initial_backoff.is_zero() || max_backoff.is_zero() {
            return Err(McpStdioConfigError::ZeroRestartBackoff);
        }
        if max_backoff < initial_backoff {
            return Err(McpStdioConfigError::InvalidRestartBackoff);
        }
        if max_backoff > MAX_AUTOMATIC_RESTART_BACKOFF {
            return Err(McpStdioConfigError::RestartBackoffLimit);
        }
        self.automatic_restart = Some(StdioAutomaticRestart {
            max_attempts,
            initial_backoff,
            max_backoff,
        });
        Ok(self)
    }
}

impl fmt::Debug for McpStdioConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpStdioConfig")
            .field("program", &self.program)
            .field("argument_count", &self.arguments.len())
            .field("max_message_bytes", &self.max_message_bytes)
            .field("max_list_pages", &self.max_list_pages)
            .field("max_tools", &self.max_tools)
            .field("max_interleaved_messages", &self.max_interleaved_messages)
            .field("max_context_items", &self.max_context_items)
            .field("max_context_bytes", &self.max_context_bytes)
            .field("request_timeout", &self.request_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field(
                "automatic_restart",
                &self.automatic_restart.map(|restart| {
                    (
                        restart.max_attempts,
                        restart.initial_backoff,
                        restart.max_backoff,
                    )
                }),
            )
            .finish()
    }
}

/// Invalid stdio MCP client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpStdioConfigError {
    #[error("MCP stdio program must not be blank")]
    BlankProgram,
    #[error("MCP stdio message byte limit must be non-zero")]
    ZeroMessageLimit,
    #[error("MCP stdio discovery and message limits must be non-zero")]
    ZeroLimit,
    #[error("MCP stdio context item and byte limits must be non-zero")]
    ZeroContextLimit,
    #[error("MCP stdio request timeout must be non-zero")]
    ZeroRequestTimeout,
    #[error("MCP stdio shutdown timeout must be non-zero")]
    ZeroShutdownTimeout,
    #[error("MCP stdio automatic restart attempts must be non-zero")]
    ZeroRestartAttempts,
    #[error("MCP stdio automatic restart attempts exceed the bounded limit")]
    RestartAttemptLimit,
    #[error("MCP stdio automatic restart backoff values must be non-zero")]
    ZeroRestartBackoff,
    #[error("MCP stdio automatic restart maximum backoff must not be below its initial backoff")]
    InvalidRestartBackoff,
    #[error("MCP stdio automatic restart maximum backoff exceeds the bounded limit")]
    RestartBackoffLimit,
}

struct StdioConnection {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    capabilities: McpServerCapabilities,
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
        let mut connection = self.spawn()?;
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
            shutdown_connection(&mut previous, self.config.shutdown_timeout).await?;
        }
        self.state.next_request_id.store(0, Ordering::Relaxed);
        let mut connection = self.spawn()?;
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

    /// Discovers application-selected MCP resources without fetching or forwarding their content.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_resources_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_resources_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpResource>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut uris = BTreeSet::new();
        let mut resources = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "resources/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("resources")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let resource = parse_resource(value)?;
                if !uris.insert(resource.uri().as_str().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                resources.push(resource);
                if resources.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(resources);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Discovers parameterized MCP resource templates without expanding or fetching them.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_resource_templates_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_resource_templates_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpResourceTemplate>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut templates = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "resources/templates/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("resourceTemplates")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let template = parse_resource_template(value)?;
                if !names.insert(template.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                templates.push(template);
                if templates.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(templates);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Reads one explicitly selected MCP resource without adding it to an AI request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::InvalidContextRequest`] for an invalid local URI or a sanitized remote
    /// capability, transport, protocol, or bounds failure.
    pub async fn read_resource(
        &self,
        uri: &url::Url,
    ) -> Result<Vec<McpResourceContents>, McpError> {
        if !valid_context_request_string(uri.as_str(), self.config.max_context_bytes) {
            return Err(McpError::InvalidContextRequest);
        }
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.read_resource_on(connection, uri).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn read_resource_on(
        &self,
        connection: &mut StdioConnection,
        uri: &url::Url,
    ) -> Result<Vec<McpResourceContents>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let result = self
            .request_on(
                connection,
                self.next_request_id(),
                "resources/read",
                json!({"uri":uri.as_str()}),
            )
            .await?;
        let contents = result
            .get("contents")
            .and_then(Value::as_array)
            .filter(|contents| {
                !contents.is_empty() && contents.len() <= self.config.max_context_items
            })
            .ok_or(McpError::MalformedResponse)?;
        let mut total_bytes = 0_usize;
        let mut parsed = Vec::with_capacity(contents.len());
        for value in contents {
            let content = parse_resource_contents(
                value,
                self.config.max_context_bytes.saturating_sub(total_bytes),
            )?;
            if content.uri() != uri {
                return Err(McpError::MalformedResponse);
            }
            total_bytes = total_bytes.saturating_add(content.data().byte_len());
            if total_bytes > self.config.max_context_bytes {
                return Err(McpError::ContextLimit);
            }
            parsed.push(content);
        }
        Ok(parsed)
    }

    /// Discovers user-selectable MCP prompt declarations without requesting their messages.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_prompts_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_prompts_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpPrompt>, McpError> {
        if !connection.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut prompts = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "prompts/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("prompts")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let prompt = parse_prompt(value, self.config.max_context_items)?;
                if !names.insert(prompt.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                prompts.push(prompt);
                if prompts.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(prompts);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Gets one explicitly selected MCP prompt without adding it to an AI request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::InvalidContextRequest`] for unsafe local input or a sanitized remote
    /// capability, transport, protocol, or bounds failure.
    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpPromptResult, McpError> {
        if !valid_context_name(name) || arguments.len() > self.config.max_context_items {
            return Err(McpError::InvalidContextRequest);
        }
        let mut total_argument_bytes = 0_usize;
        for (key, value) in arguments {
            if !valid_context_name(key)
                || !valid_context_request_string(value, self.config.max_context_bytes)
            {
                return Err(McpError::InvalidContextRequest);
            }
            total_argument_bytes = total_argument_bytes.saturating_add(value.len());
            if total_argument_bytes > self.config.max_context_bytes {
                return Err(McpError::InvalidContextRequest);
            }
        }
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.get_prompt_on(connection, name, arguments).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn get_prompt_on(
        &self,
        connection: &mut StdioConnection,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpPromptResult, McpError> {
        if !connection.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut params = serde_json::Map::new();
        params.insert("name".to_owned(), Value::String(name.to_owned()));
        if !arguments.is_empty() {
            params.insert(
                "arguments".to_owned(),
                serde_json::to_value(arguments).map_err(|_| McpError::MalformedResponse)?,
            );
        }
        let result = self
            .request_on(
                connection,
                self.next_request_id(),
                "prompts/get",
                Value::Object(params),
            )
            .await?;
        parse_prompt_result(
            &result,
            self.config.max_context_items,
            self.config.max_context_bytes,
        )
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
        shutdown_connection(&mut connection, self.config.shutdown_timeout).await
    }

    fn spawn(&self) -> Result<StdioConnection, McpError> {
        let mut child = Command::new(&self.config.program)
            .args(&self.config.arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| McpError::StdioSpawn)?;
        Ok(StdioConnection {
            stdin: Some(child.stdin.take().ok_or(McpError::StdioSpawn)?),
            stdout: BufReader::new(child.stdout.take().ok_or(McpError::StdioSpawn)?),
            capabilities: McpServerCapabilities::default(),
            child,
        })
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
        connection.capabilities = validate_initialize(&result)?;
        self.notification_on(connection, "notifications/initialized", json!({}))
            .await
    }

    async fn notification_on(
        &self,
        connection: &mut StdioConnection,
        method: &str,
        params: Value,
    ) -> Result<(), McpError> {
        timeout(
            self.config.request_timeout,
            self.write_message(
                connection,
                json!({"jsonrpc":"2.0","method":method,"params":params}),
            ),
        )
        .await
        .map_err(|_| McpError::StdioTimeout)?
    }

    async fn request_on(
        &self,
        connection: &mut StdioConnection,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, McpError> {
        timeout(
            self.config.request_timeout,
            self.request_on_with_deadline(connection, id, method, params),
        )
        .await
        .map_err(|_| McpError::StdioTimeout)?
    }

    async fn request_on_with_deadline(
        &self,
        connection: &mut StdioConnection,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, McpError> {
        self.write_message(
            connection,
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )
        .await?;
        for _ in 0..self.config.max_interleaved_messages {
            let line = read_line(&mut connection.stdout, self.config.max_message_bytes)
                .await?
                .ok_or(McpError::StdioTerminated)?;
            if line.is_empty() {
                continue;
            }
            let value =
                serde_json::from_slice::<Value>(&line).map_err(|_| McpError::MalformedResponse)?;
            if value.get("id").is_some() {
                return decode_rpc_result(&value, id);
            }
            if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
                || value.get("method").and_then(Value::as_str).is_none()
            {
                return Err(McpError::MalformedResponse);
            }
        }
        Err(McpError::StdioMessageLimit)
    }

    async fn write_message(
        &self,
        connection: &mut StdioConnection,
        value: Value,
    ) -> Result<(), McpError> {
        let encoded = serde_json::to_vec(&value).map_err(|_| McpError::MalformedResponse)?;
        if encoded.len() > self.config.max_message_bytes {
            return Err(McpError::StdioRequestTooLarge);
        }
        connection
            .stdin
            .as_mut()
            .ok_or(McpError::Transport)?
            .write_all(&encoded)
            .await
            .map_err(|_| McpError::Transport)?;
        connection
            .stdin
            .as_mut()
            .ok_or(McpError::Transport)?
            .write_all(b"\n")
            .await
            .map_err(|_| McpError::Transport)?;
        connection
            .stdin
            .as_mut()
            .ok_or(McpError::Transport)?
            .flush()
            .await
            .map_err(|_| McpError::Transport)
    }

    async fn finish_request<T>(
        &self,
        result: Result<T, McpError>,
        stale: Option<StdioConnection>,
    ) -> Result<T, McpError> {
        let Some(mut stale) = stale else {
            return result;
        };
        if shutdown_connection(&mut stale, self.config.shutdown_timeout)
            .await
            .is_ok()
        {
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
            let Ok(mut connection) = self.spawn() else {
                continue;
            };
            if self.initialize_connection(&mut connection).await.is_ok() {
                *self.state.connection.lock().await = Some(connection);
                return;
            }
            let _ = shutdown_connection(&mut connection, self.config.shutdown_timeout).await;
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

async fn shutdown_connection(
    connection: &mut StdioConnection,
    shutdown_timeout: Duration,
) -> Result<(), McpError> {
    let stdin_result = match connection.stdin.take() {
        Some(mut stdin) => stdin.shutdown().await.map_err(|_| McpError::Transport),
        None => Ok(()),
    };
    match timeout(shutdown_timeout, connection.child.wait()).await {
        Ok(result) => {
            result.map_err(|_| McpError::Transport)?;
        }
        Err(_) => {
            if connection
                .child
                .try_wait()
                .map_err(|_| McpError::Transport)?
                .is_none()
            {
                connection
                    .child
                    .start_kill()
                    .map_err(|_| McpError::Transport)?;
                timeout(shutdown_timeout, connection.child.wait())
                    .await
                    .map_err(|_| McpError::StdioShutdownTimeout)?
                    .map_err(|_| McpError::Transport)?;
            }
        }
    }
    stdin_result
}

impl fmt::Debug for McpStdioClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpStdioClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Selected stdio tool executor that stays behind Rustee approval policy.
#[derive(Clone)]
pub struct McpStdioRemoteTool {
    client: McpStdioClient,
    definition: ToolDefinition,
    risk: ToolRisk,
    forward_idempotency_key: bool,
}
impl McpStdioRemoteTool {
    #[must_use]
    pub fn from_discovery(
        client: McpStdioClient,
        discovered: McpToolDefinition,
        risk: ToolRisk,
    ) -> Self {
        Self {
            client,
            definition: discovered.definition,
            risk,
            forward_idempotency_key: false,
        }
    }
    #[must_use]
    pub const fn with_rustee_idempotency_metadata(mut self) -> Self {
        self.forward_idempotency_key = true;
        self
    }
}
impl ToolExecutor for McpStdioRemoteTool {
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }
    fn risk(&self) -> ToolRisk {
        self.risk
    }
    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>> {
        let client = self.client.clone();
        let name = self.definition.name().to_owned();
        let key = self
            .forward_idempotency_key
            .then(|| context.idempotency_key().to_owned());
        Box::pin(async move {
            client
                .call_tool(name, arguments, key)
                .await
                .map_err(|_| ToolExecutionError::HandlerFailed)
        })
    }
}
impl fmt::Debug for McpStdioRemoteTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpStdioRemoteTool")
            .field("name", &self.definition.name())
            .field("risk", &self.risk)
            .field("forward_idempotency_key", &self.forward_idempotency_key)
            .finish_non_exhaustive()
    }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        convert::Infallible,
        fs,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use rustee_ai::{
        AiExecutionContext, ToolApprovalDecision, ToolApprovalPolicy, ToolCall,
        ToolExecutionContext, ToolRegistry, ToolRisk, ToolRunError,
    };
    use serde_json::json;

    use super::{
        McpStdioClient, McpStdioConfig, McpStdioConfigError, McpStdioRemoteTool,
        StdioAutomaticRestart,
    };
    use crate::{MCP_PROTOCOL_VERSION, McpError, McpPromptContent, McpResourceData};

    #[test]
    fn configuration_requires_bounded_local_commands() {
        assert_eq!(
            McpStdioConfig::new("").unwrap_err(),
            McpStdioConfigError::BlankProgram
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_max_message_bytes(0)
                .unwrap_err(),
            McpStdioConfigError::ZeroMessageLimit
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_context_limits(1, 0)
                .unwrap_err(),
            McpStdioConfigError::ZeroContextLimit
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_request_timeout(Duration::ZERO)
                .unwrap_err(),
            McpStdioConfigError::ZeroRequestTimeout
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_shutdown_timeout(Duration::ZERO)
                .unwrap_err(),
            McpStdioConfigError::ZeroShutdownTimeout
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_automatic_restart(0, Duration::from_millis(1), Duration::from_millis(1))
                .unwrap_err(),
            McpStdioConfigError::ZeroRestartAttempts
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_automatic_restart(1, Duration::ZERO, Duration::from_millis(1))
                .unwrap_err(),
            McpStdioConfigError::ZeroRestartBackoff
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_automatic_restart(1, Duration::from_millis(2), Duration::from_millis(1))
                .unwrap_err(),
            McpStdioConfigError::InvalidRestartBackoff
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_automatic_restart(9, Duration::from_millis(1), Duration::from_millis(1))
                .unwrap_err(),
            McpStdioConfigError::RestartAttemptLimit
        );
        assert_eq!(
            McpStdioConfig::new("server")
                .unwrap()
                .with_automatic_restart(1, Duration::from_secs(1), Duration::from_secs(31))
                .unwrap_err(),
            McpStdioConfigError::RestartBackoffLimit
        );
    }

    #[test]
    fn automatic_restart_backoff_is_exponential_and_capped() {
        let restart = StdioAutomaticRestart {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(5),
        };
        assert_eq!(restart.delay_for(0), Duration::from_millis(2));
        assert_eq!(restart.delay_for(1), Duration::from_millis(4));
        assert_eq!(restart.delay_for(2), Duration::from_millis(5));
        assert_eq!(restart.delay_for(3), Duration::from_millis(5));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_request_discards_the_subprocess_connection() {
        let script = format!(
            "read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"{MCP_PROTOCOL_VERSION}\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"fixture\",\"version\":\"0.1.0\"}}}}}}'; read line; read line; while :; do :; done"
        );
        let client = McpStdioClient::new(
            McpStdioConfig::new("sh")
                .unwrap()
                .with_arguments(["-c", script.as_str()])
                .with_request_timeout(Duration::from_millis(10))
                .unwrap()
                .with_shutdown_timeout(Duration::from_millis(10))
                .unwrap(),
        );
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::StdioTimeout
        );
        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::NotInitialized
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explicit_restart_replaces_the_stdio_process_without_replaying_a_call() {
        let script = format!(
            "read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"{MCP_PROTOCOL_VERSION}\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"fixture\",\"version\":\"0.1.0\"}}}}}}'; read line; read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{{\"tools\":[{{\"name\":\"orders.restarted\",\"inputSchema\":{{\"type\":\"object\"}}}}]}}}}'"
        );
        let client = McpStdioClient::new(
            McpStdioConfig::new("sh")
                .unwrap()
                .with_arguments(["-c", script.as_str()]),
        );
        client.initialize().await.unwrap();
        client.restart().await.unwrap();

        let discovered = client.list_tools().await.unwrap();
        assert_eq!(discovered[0].name(), "orders.restarted");
        client.close().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn automatic_restart_prepares_a_new_process_without_replaying_a_tool_call() {
        let state_path = std::env::temp_dir().join(format!(
            "rustee-ai-mcp-auto-restart-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let state = state_path.to_str().unwrap();
        let initialize = stdio_reply(
            1,
            &json!({
                "protocolVersion":MCP_PROTOCOL_VERSION,
                "capabilities":{},
                "serverInfo":{"name":"fixture","version":"0.1.0"}
            }),
        );
        let discovery = stdio_reply(
            2,
            &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
        );
        let script = format!(
            "state=$0; if [ -f \"$state\" ]; then restarted=1; else : > \"$state\"; restarted=0; fi; read line; printf '%s\\n' '{initialize}'; read line; if [ \"$restarted\" -eq 0 ]; then read line; printf '%s\\n' '{discovery}'; read line; printf '%s\\n' 'tool-call' >> \"$state\"; exit 0; fi; read line; printf '%s\\n' 'second-request' >> \"$state\"; printf '%s\\n' '{discovery}'"
        );
        let client = McpStdioClient::new(
            McpStdioConfig::new("sh")
                .unwrap()
                .with_arguments(["-c", script.as_str(), state])
                .with_automatic_restart(1, Duration::from_millis(1), Duration::from_millis(1))
                .unwrap(),
        );
        client.initialize().await.unwrap();
        let tool = client.list_tools().await.unwrap().remove(0);
        let mut registry = ToolRegistry::new();
        registry
            .register(McpStdioRemoteTool::from_discovery(
                client.clone(),
                tool,
                ToolRisk::ReadOnly,
            ))
            .unwrap();

        let error = registry
            .execute(
                ToolExecutionContext::new(
                    AiExecutionContext::new("tenant-a", "user-7").unwrap(),
                    "automatic-restart-action",
                )
                .unwrap(),
                ToolCall::new("call-1", "orders.recovered", json!({})).unwrap(),
                &Approve,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolRunError::Execution(_)));
        assert_eq!(fs::read_to_string(&state_path).unwrap(), "tool-call\n");

        let recovered = client.list_tools().await.unwrap();
        assert_eq!(recovered[0].name(), "orders.recovered");
        assert_eq!(
            fs::read_to_string(&state_path).unwrap(),
            "tool-call\nsecond-request\n"
        );
        client.close().await.unwrap();
        fs::remove_file(state_path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_context_discovery_and_reads_stay_explicit_and_bounded() {
        let initialize = stdio_reply(
            1,
            &json!({
                "protocolVersion":MCP_PROTOCOL_VERSION,
                "capabilities":{"resources":{},"prompts":{}},
                "serverInfo":{"name":"fixture","version":"0.1.0"}
            }),
        );
        let resources = stdio_reply(
            2,
            &json!({"resources":[{
                "uri":"resource://tenant-a/customer/7",
                "name":"customer-record",
                "mimeType":"text/plain"
            }]}),
        );
        let templates = stdio_reply(
            3,
            &json!({"resourceTemplates":[{
                "uriTemplate":"resource://tenant-a/customer/{id}",
                "name":"customer-by-id"
            }]}),
        );
        let contents = stdio_reply(
            4,
            &json!({"contents":[{
                "uri":"resource://tenant-a/customer/7",
                "text":"private customer context"
            }]}),
        );
        let prompts = stdio_reply(
            5,
            &json!({"prompts":[{
                "name":"customer-summary",
                "arguments":[{"name":"customer_id","required":true}]
            }]}),
        );
        let prompt = stdio_reply(
            6,
            &json!({"messages":[
                {"role":"user","content":{"type":"text","text":"Summarize the selected customer."}},
                {"role":"assistant","content":{"type":"resource_link","uri":"resource://tenant-a/customer/7","name":"customer-record"}}
            ]}),
        );
        let script = format!(
            "read line; printf '%s\\n' '{initialize}'; read line; read line; printf '%s\\n' '{resources}'; read line; printf '%s\\n' '{templates}'; read line; printf '%s\\n' '{contents}'; read line; printf '%s\\n' '{prompts}'; read line; printf '%s\\n' '{prompt}'"
        );
        let client = McpStdioClient::new(
            McpStdioConfig::new("sh")
                .unwrap()
                .with_arguments(["-c", script.as_str()]),
        );
        client.initialize().await.unwrap();

        let resources = client.list_resources().await.unwrap();
        assert_eq!(resources[0].name(), "customer-record");
        let templates = client.list_resource_templates().await.unwrap();
        assert_eq!(templates[0].name(), "customer-by-id");
        let contents = client.read_resource(resources[0].uri()).await.unwrap();
        assert!(matches!(
            contents[0].data(),
            McpResourceData::Text(text) if text == "private customer context"
        ));
        let prompts = client.list_prompts().await.unwrap();
        assert!(prompts[0].arguments()[0].required());
        let prompt = client
            .get_prompt(
                "customer-summary",
                &BTreeMap::from([("customer_id".to_owned(), "7".to_owned())]),
            )
            .await
            .unwrap();
        assert!(matches!(
            prompt.messages()[0].content(),
            McpPromptContent::Text(text) if text == "Summarize the selected customer."
        ));
        assert!(!format!("{prompt:?}").contains("Summarize the selected customer."));
        client.close().await.unwrap();
    }

    fn stdio_reply(id: u64, result: &serde_json::Value) -> String {
        json!({"jsonrpc":"2.0","id":id,"result":result}).to_string()
    }

    #[derive(Clone, Copy)]
    struct Approve;
    impl ToolApprovalPolicy for Approve {
        type Error = Infallible;
        fn approve(
            &self,
            _: AiExecutionContext,
            _: ToolCall,
            _: ToolRisk,
        ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>
        {
            Box::pin(futures_util::future::ready(Ok(
                ToolApprovalDecision::Approved,
            )))
        }
    }
}
