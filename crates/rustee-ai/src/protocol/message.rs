//! Chat message values and role validation.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::RequestError;

/// The role of one chat message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// System instruction.
    System,
    /// End-user input.
    User,
    /// Model output.
    Assistant,
    /// An approved tool result.
    Tool,
}

/// One message supplied to a provider.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ChatMessage {
    role: MessageRole,
    content: String,
}

#[derive(Deserialize)]
struct SerializedChatMessage {
    role: MessageRole,
    content: String,
}

impl<'de> Deserialize<'de> for ChatMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedChatMessage::deserialize(deserializer)?;
        Self::new(serialized.role, serialized.content).map_err(serde::de::Error::custom)
    }
}

impl ChatMessage {
    /// Creates a message with non-blank content.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError::BlankMessage`] when `content` is blank.
    pub fn new(role: MessageRole, content: impl Into<String>) -> Result<Self, RequestError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(RequestError::BlankMessage);
        }
        Ok(Self { role, content })
    }

    /// Returns the message role.
    #[must_use]
    pub const fn role(&self) -> MessageRole {
        self.role
    }

    /// Returns message content. Do not record this value in logs by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }
}

impl fmt::Debug for ChatMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatMessage")
            .field("role", &self.role)
            .field("content_length", &self.content.len())
            .finish()
    }
}
