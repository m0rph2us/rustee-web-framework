//! Provider response values and content-safe structured output parsing.

use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    ResponseError,
    error::StructuredOutputError,
    request::{ModelAliasError, validate_model_alias},
    tool::ToolCall,
};

/// Provider-reported token usage.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Usage {
    /// Input token count.
    pub input_tokens: u64,
    /// Output token count.
    pub output_tokens: u64,
}

impl Usage {
    /// Returns total token use.
    #[must_use]
    pub const fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

/// Completed response from a provider.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ChatResponse {
    id: String,
    model: String,
    content: String,
    tool_calls: Vec<ToolCall>,
    usage: Usage,
}

#[derive(Deserialize)]
struct SerializedChatResponse {
    id: String,
    model: String,
    content: String,
    tool_calls: Vec<ToolCall>,
    usage: Usage,
}

impl<'de> Deserialize<'de> for ChatResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedChatResponse::deserialize(deserializer)?;
        Self::new(
            serialized.id,
            serialized.model,
            serialized.content,
            serialized.tool_calls,
            serialized.usage,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ChatResponse {
    /// Creates a provider response with a non-blank ID and a bounded model alias.
    ///
    /// # Errors
    ///
    /// Returns [`ResponseError`] when provider metadata is blank.
    pub fn new(
        id: impl Into<String>,
        model: impl Into<String>,
        content: impl Into<String>,
        tool_calls: impl IntoIterator<Item = ToolCall>,
        usage: Usage,
    ) -> Result<Self, ResponseError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(ResponseError::BlankId);
        }
        let model = model.into();
        validate_model_alias(&model).map_err(response_model_alias_error)?;
        Ok(Self {
            id,
            model,
            content: content.into(),
            tool_calls: tool_calls.into_iter().collect(),
            usage,
        })
    }

    /// Returns provider response ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns resolved provider model.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns untrusted text content.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns requested but unexecuted tool calls.
    #[must_use]
    pub fn tool_calls(&self) -> &[ToolCall] {
        &self.tool_calls
    }

    /// Returns provider usage.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }

    /// Deserializes text content as structured JSON.
    ///
    /// # Errors
    ///
    /// Returns [`StructuredOutputError`] when the model content is not valid JSON for `T`.
    pub fn parse_json<T>(&self) -> Result<T, StructuredOutputError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(&self.content).map_err(StructuredOutputError::deserialize)
    }
}

fn response_model_alias_error(error: ModelAliasError) -> ResponseError {
    match error {
        ModelAliasError::Blank => ResponseError::BlankModel,
        ModelAliasError::TooLong => ResponseError::ModelAliasTooLong,
        ModelAliasError::ContainsNul => ResponseError::ModelAliasContainsNul,
    }
}

impl fmt::Debug for ChatResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatResponse")
            .field("id", &"[REDACTED]")
            .field("model", &self.model)
            .field("content_length", &self.content.len())
            .field("tool_call_count", &self.tool_calls.len())
            .field("usage", &self.usage)
            .finish()
    }
}
