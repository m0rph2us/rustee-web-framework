//! MCP server metadata, payload bounds, and browser-origin admission.

use std::{collections::BTreeSet, fmt};

use http::header::ORIGIN;
use rustee_core::Request;
use url::Url;

use crate::header::{HeaderAdmission, admit_single_header};

const DEFAULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_TOOL_ITEMS: usize = 128;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
/// Maximum number of distinct canonical browser origins accepted by one MCP server.
pub const MAX_ALLOWED_ORIGINS: usize = 128;

/// Public server identity, request bounds, and browser-origin admission.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerConfig {
    pub(super) server_name: String,
    pub(super) server_version: String,
    pub(super) max_request_bytes: usize,
    pub(super) max_response_bytes: usize,
    pub(super) max_tool_items: usize,
    pub(super) max_context_items: usize,
    allowed_origins: BTreeSet<String>,
}

impl McpServerConfig {
    /// Creates server metadata advertised in a successful MCP initialization response.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::InvalidServerInfo`] when either value is blank or contains
    /// a NUL byte.
    pub fn new(
        server_name: impl Into<String>,
        server_version: impl Into<String>,
    ) -> Result<Self, McpServerConfigError> {
        let server_name = server_name.into();
        let server_version = server_version.into();
        if invalid_server_info(&server_name) || invalid_server_info(&server_version) {
            return Err(McpServerConfigError::InvalidServerInfo);
        }
        Ok(Self {
            server_name,
            server_version,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_tool_items: DEFAULT_MAX_TOOL_ITEMS,
            max_context_items: DEFAULT_MAX_CONTEXT_ITEMS,
            allowed_origins: BTreeSet::new(),
        })
    }

    /// Sets the maximum JSON-RPC request body collected by this service.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroRequestLimit`] when `max_request_bytes` is zero.
    pub fn with_max_request_bytes(
        mut self,
        max_request_bytes: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_request_bytes == 0 {
            return Err(McpServerConfigError::ZeroRequestLimit);
        }
        self.max_request_bytes = max_request_bytes;
        Ok(self)
    }

    /// Sets the maximum successful JSON-RPC response body emitted by this service.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroResponseLimit`] when `max_response_bytes` is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_response_bytes == 0 {
            return Err(McpServerConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    /// Sets the maximum visible tools returned by one `tools/list` operation.
    ///
    /// The byte size of the resulting response remains subject to `max_response_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroToolItemLimit`] when `max_tool_items` is zero.
    pub fn with_max_tool_items(
        mut self,
        max_tool_items: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_tool_items == 0 {
            return Err(McpServerConfigError::ZeroToolItemLimit);
        }
        self.max_tool_items = max_tool_items;
        Ok(self)
    }

    /// Sets the maximum items returned by one context-provider operation.
    ///
    /// The byte size of the resulting response remains subject to `max_response_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroContextItemLimit`] when `max_context_items` is zero.
    pub fn with_max_context_items(
        mut self,
        max_context_items: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_context_items == 0 {
            return Err(McpServerConfigError::ZeroContextItemLimit);
        }
        self.max_context_items = max_context_items;
        Ok(self)
    }

    /// Replaces the exact HTTP(S) origins accepted when an `Origin` header is present.
    ///
    /// The empty default intentionally rejects every request that carries `Origin`, while native
    /// MCP clients without that header continue through normal authentication. Origins are
    /// normalized to scheme/host/port and may not contain a path, query, fragment, or credentials.
    /// This is a DNS-rebinding defense, not an authentication or CORS policy.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::InvalidAllowedOrigin`] when any origin is not a valid
    /// absolute HTTP(S) origin or [`McpServerConfigError::AllowedOriginLimit`] when more than
    /// [`MAX_ALLOWED_ORIGINS`] distinct canonical origins are supplied.
    pub fn with_allowed_origins<Origins, Origin>(
        mut self,
        origins: Origins,
    ) -> Result<Self, McpServerConfigError>
    where
        Origins: IntoIterator<Item = Origin>,
        Origin: AsRef<str>,
    {
        let mut allowed_origins = BTreeSet::new();
        for origin in origins {
            let origin = canonical_origin(origin.as_ref())
                .ok_or(McpServerConfigError::InvalidAllowedOrigin)?;
            if !allowed_origins.contains(&origin) {
                if allowed_origins.len() == MAX_ALLOWED_ORIGINS {
                    return Err(McpServerConfigError::AllowedOriginLimit);
                }
                allowed_origins.insert(origin);
            }
        }
        self.allowed_origins = allowed_origins;
        Ok(self)
    }

    pub(super) fn allows_origin(&self, request: &Request) -> bool {
        match admit_single_header(request.headers(), ORIGIN) {
            HeaderAdmission::Missing => true,
            HeaderAdmission::Valid(origin) => canonical_origin(origin)
                .is_some_and(|origin| self.allowed_origins.contains(&origin)),
            HeaderAdmission::Invalid => false,
        }
    }
}

impl fmt::Debug for McpServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerConfig")
            .field("server_name", &self.server_name)
            .field("server_version", &self.server_version)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_tool_items", &self.max_tool_items)
            .field("max_context_items", &self.max_context_items)
            .field("allowed_origin_count", &self.allowed_origins.len())
            .finish()
    }
}

/// Invalid public MCP server configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpServerConfigError {
    /// Public server name or version was blank or malformed.
    #[error("MCP server name and version must be non-blank and valid")]
    InvalidServerInfo,
    /// The request reader needs a positive byte limit.
    #[error("MCP request byte limit must be non-zero")]
    ZeroRequestLimit,
    /// The response writer needs a positive byte limit.
    #[error("MCP response byte limit must be non-zero")]
    ZeroResponseLimit,
    /// Tool discovery needs a positive item limit.
    #[error("MCP tool item limit must be non-zero")]
    ZeroToolItemLimit,
    /// A context provider needs a positive per-operation item limit.
    #[error("MCP context item limit must be non-zero")]
    ZeroContextItemLimit,
    /// Too many distinct browser origins were configured.
    #[error("MCP allowed-origin list exceeds the bounded limit")]
    AllowedOriginLimit,
    /// An explicit browser origin was not a canonical HTTP(S) origin.
    #[error("MCP allowed origin must be an absolute HTTP(S) origin without a path or credentials")]
    InvalidAllowedOrigin,
}

fn invalid_server_info(value: &str) -> bool {
    value.trim().is_empty() || value.contains('\0')
}

fn canonical_origin(value: &str) -> Option<String> {
    if value.trim() != value {
        return None;
    }
    let url = Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let origin = url.origin().ascii_serialization();
    (origin != "null").then_some(origin)
}
