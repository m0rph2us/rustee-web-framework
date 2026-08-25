//! Provider-neutral chat request values and deployment model aliases.

use std::{fmt, io};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ToolResult;

use super::{RequestError, message::ChatMessage, tool::ToolDefinition};

/// Maximum UTF-8 byte length accepted for a durable deployment-owned model alias.
pub const MAX_MODEL_ALIAS_BYTES: usize = 255;

/// A request expressed through deployment-owned model alias and application messages.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    tools: Vec<ToolDefinition>,
    tool_results: Vec<ToolResult>,
}

#[derive(Deserialize)]
struct SerializedChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    tools: Vec<ToolDefinition>,
    tool_results: Vec<ToolResult>,
}

impl<'de> Deserialize<'de> for ChatRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedChatRequest::deserialize(deserializer)?;
        Self::new(serialized.model, serialized.messages)
            .map(|request| {
                request
                    .with_tools(serialized.tools)
                    .with_tool_results(serialized.tool_results)
            })
            .map_err(serde::de::Error::custom)
    }
}

impl ChatRequest {
    /// Creates a non-empty request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] when the alias or messages are invalid.
    pub fn new(
        model: impl Into<String>,
        messages: impl IntoIterator<Item = ChatMessage>,
    ) -> Result<Self, RequestError> {
        let model = model.into();
        validate_model_alias(&model).map_err(request_model_alias_error)?;
        let messages = messages.into_iter().collect::<Vec<_>>();
        if messages.is_empty() {
            return Err(RequestError::EmptyMessages);
        }
        Ok(Self {
            model,
            messages,
            tools: Vec::new(),
            tool_results: Vec::new(),
        })
    }

    /// Adds manually executed tool declarations.
    #[must_use]
    pub fn with_tools(mut self, tools: impl IntoIterator<Item = ToolDefinition>) -> Self {
        self.tools = tools.into_iter().collect();
        self
    }

    /// Appends one validated application context message.
    ///
    /// Advisors use this to add authorized retrieval or policy context. The pipeline validates the
    /// final request after every advisor has run and before it reaches a provider.
    #[must_use]
    pub fn with_added_message(mut self, message: ChatMessage) -> Self {
        self.messages.push(message);
        self
    }

    /// Adds application-approved results for provider-specific function-call continuation.
    #[must_use]
    pub fn with_tool_results(mut self, tool_results: impl IntoIterator<Item = ToolResult>) -> Self {
        self.tool_results = tool_results.into_iter().collect();
        self
    }

    /// Returns the model alias.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns messages in request order.
    #[must_use]
    pub fn messages(&self) -> &[ChatMessage] {
        &self.messages
    }

    /// Returns declared tools.
    #[must_use]
    pub fn tools(&self) -> &[ToolDefinition] {
        &self.tools
    }

    /// Returns application-approved tool results that are safe to send back to a provider.
    #[must_use]
    pub fn tool_results(&self) -> &[ToolResult] {
        &self.tool_results
    }

    /// Returns the aggregate Unicode scalar count of provider-bound input content.
    ///
    /// Tool result values use their compact JSON form because providers receive those values as
    /// serialized function-call output.
    pub(crate) fn input_characters(&self) -> usize {
        self.messages
            .iter()
            .map(|message| message.content().chars().count())
            .chain(
                self.tool_results
                    .iter()
                    .map(|result| serialized_json_characters(result.content())),
            )
            .fold(0, usize::saturating_add)
    }
}

/// Counts Unicode scalar values in one compact JSON value without allocating its serialized form.
fn serialized_json_characters(value: &Value) -> usize {
    let mut counter = JsonCharacterCounter::default();
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.characters,
        // `Value` and this writer are infallible today. If either contract changes, fail closed
        // so a request never bypasses the configured input policy.
        Err(_) => usize::MAX,
    }
}

#[derive(Default)]
struct JsonCharacterCounter {
    characters: usize,
}

impl io::Write for JsonCharacterCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        // UTF-8 continuation bytes never start a Unicode scalar, even if a serializer write
        // splits a scalar across buffers.
        let characters = buffer
            .iter()
            .filter(|byte| **byte & 0b1100_0000 != 0b1000_0000)
            .count();
        self.characters = self.characters.saturating_add(characters);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Invalid deployment-owned model alias metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ModelAliasError {
    /// The alias was blank.
    #[error("AI model alias must not be blank")]
    Blank,
    /// The alias exceeded the durable metadata limit.
    #[error("AI model alias exceeded the supported length")]
    TooLong,
    /// The alias contained a NUL byte.
    #[error("AI model alias must not contain a NUL byte")]
    ContainsNul,
}

/// Validates the stable model alias used by provider requests and durable AI metadata.
///
/// # Errors
///
/// Returns [`ModelAliasError`] when `model` is blank, contains a NUL byte, or exceeds
/// [`MAX_MODEL_ALIAS_BYTES`].
pub fn validate_model_alias(model: &str) -> Result<(), ModelAliasError> {
    if model.trim().is_empty() {
        return Err(ModelAliasError::Blank);
    }
    if model.len() > MAX_MODEL_ALIAS_BYTES {
        return Err(ModelAliasError::TooLong);
    }
    if model.contains('\0') {
        return Err(ModelAliasError::ContainsNul);
    }
    Ok(())
}

fn request_model_alias_error(error: ModelAliasError) -> RequestError {
    match error {
        ModelAliasError::Blank => RequestError::BlankModel,
        ModelAliasError::TooLong => RequestError::ModelAliasTooLong,
        ModelAliasError::ContainsNul => RequestError::ModelAliasContainsNul,
    }
}

impl fmt::Debug for ChatRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatRequest")
            .field("model", &self.model)
            .field("messages", &self.messages)
            .field("tool_count", &self.tools.len())
            .field("tool_result_count", &self.tool_results.len())
            .finish()
    }
}
