//! Trusted local subprocess settings and bounded automatic-restart policy.

use std::{ffi::OsString, fmt, path::PathBuf, time::Duration};

use crate::recovery::{AutomaticRecovery, AutomaticRecoveryPolicyError};

const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_LIST_PAGES: usize = 16;
const DEFAULT_MAX_TOOLS: usize = 128;
const DEFAULT_MAX_INTERLEAVED_MESSAGES: usize = 32;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
const DEFAULT_MAX_CONTEXT_BYTES: usize = 512 * 1024;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of arguments accepted for one trusted stdio subprocess command.
pub const MAX_STDIO_ARGUMENT_COUNT: usize = 128;
/// Maximum combined platform-encoded byte length accepted for stdio command arguments.
pub const MAX_STDIO_ARGUMENT_BYTES: usize = 64 * 1024;

/// Explicit local subprocess configuration for one stdio MCP server.
#[derive(Clone)]
pub struct McpStdioConfig {
    pub(super) program: PathBuf,
    pub(super) arguments: Vec<OsString>,
    pub(super) max_message_bytes: usize,
    pub(super) max_list_pages: usize,
    pub(super) max_tools: usize,
    pub(super) max_interleaved_messages: usize,
    pub(super) max_context_items: usize,
    pub(super) max_context_bytes: usize,
    pub(super) request_timeout: Duration,
    pub(super) shutdown_timeout: Duration,
    pub(super) automatic_restart: Option<AutomaticRecovery>,
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

    /// Replaces the exact local command arguments.
    ///
    /// Arguments are bounded before storage so a deployment-derived command cannot turn into an
    /// unbounded subprocess-spawn input. Debug output never includes their contents.
    ///
    /// # Errors
    ///
    /// Returns [`McpStdioConfigError::ArgumentCountLimit`] when more than
    /// [`MAX_STDIO_ARGUMENT_COUNT`] arguments are supplied,
    /// [`McpStdioConfigError::ArgumentByteLimit`] when their combined platform-encoded length
    /// exceeds [`MAX_STDIO_ARGUMENT_BYTES`], or [`McpStdioConfigError::InvalidArgument`] when an
    /// argument contains a NUL byte.
    pub fn with_arguments<Arguments, Argument>(
        mut self,
        arguments: Arguments,
    ) -> Result<Self, McpStdioConfigError>
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<OsString>,
    {
        let mut collected = Vec::new();
        let mut argument_bytes = 0_usize;
        for argument in arguments {
            let argument = argument.into();
            let bytes = argument.as_encoded_bytes();
            if bytes.contains(&0) {
                return Err(McpStdioConfigError::InvalidArgument);
            }
            if collected.len() >= MAX_STDIO_ARGUMENT_COUNT {
                return Err(McpStdioConfigError::ArgumentCountLimit);
            }
            argument_bytes = argument_bytes
                .checked_add(bytes.len())
                .filter(|total| *total <= MAX_STDIO_ARGUMENT_BYTES)
                .ok_or(McpStdioConfigError::ArgumentByteLimit)?;
            collected.push(argument);
        }
        self.arguments = collected;
        Ok(self)
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
        self.automatic_restart = Some(
            AutomaticRecovery::new(max_attempts, initial_backoff, max_backoff)
                .map_err(restart_config_error)?,
        );
        Ok(self)
    }
}

fn restart_config_error(error: AutomaticRecoveryPolicyError) -> McpStdioConfigError {
    match error {
        AutomaticRecoveryPolicyError::ZeroAttempts => McpStdioConfigError::ZeroRestartAttempts,
        AutomaticRecoveryPolicyError::AttemptLimit => McpStdioConfigError::RestartAttemptLimit,
        AutomaticRecoveryPolicyError::ZeroBackoff => McpStdioConfigError::ZeroRestartBackoff,
        AutomaticRecoveryPolicyError::InvalidBackoff => McpStdioConfigError::InvalidRestartBackoff,
        AutomaticRecoveryPolicyError::BackoffLimit => McpStdioConfigError::RestartBackoffLimit,
    }
}

impl fmt::Debug for McpStdioConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpStdioConfig")
            .field("program", &"[REDACTED]")
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
    /// The configured executable was blank.
    #[error("MCP stdio program must not be blank")]
    BlankProgram,
    /// The configured command supplied too many arguments.
    #[error("MCP stdio command argument count exceeds the bounded limit")]
    ArgumentCountLimit,
    /// The configured command arguments were too large after platform encoding.
    #[error("MCP stdio command argument bytes exceed the bounded limit")]
    ArgumentByteLimit,
    /// A configured command argument cannot be passed safely to a subprocess.
    #[error("MCP stdio command arguments must not contain NUL bytes")]
    InvalidArgument,
    /// The per-message byte bound was zero.
    #[error("MCP stdio message byte limit must be non-zero")]
    ZeroMessageLimit,
    /// A discovery page, tool, or interleaved-message limit was zero.
    #[error("MCP stdio discovery and message limits must be non-zero")]
    ZeroLimit,
    /// A context item or byte limit was zero.
    #[error("MCP stdio context item and byte limits must be non-zero")]
    ZeroContextLimit,
    /// The per-request deadline was zero.
    #[error("MCP stdio request timeout must be non-zero")]
    ZeroRequestTimeout,
    /// The forced-shutdown deadline was zero.
    #[error("MCP stdio shutdown timeout must be non-zero")]
    ZeroShutdownTimeout,
    /// Automatic restart was configured with no attempts.
    #[error("MCP stdio automatic restart attempts must be non-zero")]
    ZeroRestartAttempts,
    /// Automatic restart attempts exceeded the fixed safety bound.
    #[error("MCP stdio automatic restart attempts exceed the bounded limit")]
    RestartAttemptLimit,
    /// An automatic-restart backoff duration was zero.
    #[error("MCP stdio automatic restart backoff values must be non-zero")]
    ZeroRestartBackoff,
    /// The maximum restart backoff was below the initial backoff.
    #[error("MCP stdio automatic restart maximum backoff must not be below its initial backoff")]
    InvalidRestartBackoff,
    /// The maximum restart backoff exceeded the fixed safety bound.
    #[error("MCP stdio automatic restart maximum backoff exceeds the bounded limit")]
    RestartBackoffLimit,
}
