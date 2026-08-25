//! Application-owned read-only MCP context authorization boundary.

use std::{collections::BTreeMap, error::Error as StdError};

use rustee_core::Request;
use url::Url;

use super::{
    McpServerPrompt, McpServerPromptResult, McpServerResource, McpServerResourceContents,
    McpServerResourceTemplate,
};

/// Optional MCP features owned by an application's read-only context provider.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct McpContextCapabilities {
    resources: bool,
    prompts: bool,
}

impl McpContextCapabilities {
    /// Enables the read-only MCP resource methods.
    #[must_use]
    pub const fn with_resources(mut self) -> Self {
        self.resources = true;
        self
    }

    /// Enables the read-only MCP prompt methods.
    #[must_use]
    pub const fn with_prompts(mut self) -> Self {
        self.prompts = true;
        self
    }

    pub(crate) const fn resources(self) -> bool {
        self.resources
    }

    pub(crate) const fn prompts(self) -> bool {
        self.prompts
    }
}

/// Application trust boundary for optional, read-only MCP resources and prompts.
///
/// This provider receives the authenticated request after origin and request-size admission. It
/// is intentionally separate from [`crate::McpToolAccessPolicy`]: exposing context neither
/// invokes a tool nor grants a tool execution capability. Applications must perform tenant, user,
/// and data authorization here, and must return only data safe to disclose to the remote MCP
/// client.
pub trait McpContextProvider: Clone + Send + Sync + 'static {
    /// Application provider failure.
    type Error: StdError + Send + Sync + 'static;

    /// Declares which optional method families this provider serves.
    fn capabilities(&self) -> McpContextCapabilities;

    /// Lists the visible concrete resources for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when resource visibility cannot be resolved.
    fn list_resources(&self, request: &Request) -> Result<Vec<McpServerResource>, Self::Error>;

    /// Lists visible parameterized resource templates for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when resource-template visibility cannot be
    /// resolved.
    fn list_resource_templates(
        &self,
        request: &Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error>;

    /// Reads a resource explicitly selected by the remote client.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when the selected resource cannot be authorized
    /// or read.
    fn read_resource(
        &self,
        request: &Request,
        uri: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error>;

    /// Lists visible prompt declarations for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when prompt visibility cannot be resolved.
    fn list_prompts(&self, request: &Request) -> Result<Vec<McpServerPrompt>, Self::Error>;

    /// Returns an explicitly selected, application-authorized prompt result.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when the selected prompt cannot be authorized or
    /// resolved.
    fn get_prompt(
        &self,
        request: &Request,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error>;
}

/// Fail-closed default that keeps MCP resources and prompts undiscoverable.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllMcpContextProvider;

/// Unreachable context request under [`DenyAllMcpContextProvider`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("MCP context access is not permitted")]
pub struct DenyAllMcpContextProviderError;

impl McpContextProvider for DenyAllMcpContextProvider {
    type Error = DenyAllMcpContextProviderError;

    fn capabilities(&self) -> McpContextCapabilities {
        McpContextCapabilities::default()
    }

    fn list_resources(&self, _: &Request) -> Result<Vec<McpServerResource>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn list_resource_templates(
        &self,
        _: &Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn read_resource(
        &self,
        _: &Request,
        _: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn list_prompts(&self, _: &Request) -> Result<Vec<McpServerPrompt>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn get_prompt(
        &self,
        _: &Request,
        _: &str,
        _: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }
}
