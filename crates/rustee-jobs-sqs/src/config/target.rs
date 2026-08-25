//! Deployment queue target and FIFO message-group admission.

use std::fmt;

use url::Url;

use super::ConfigError;

const MAX_QUEUE_URL_BYTES: usize = 1_024;
const MAX_FIFO_IDENTIFIER_BYTES: usize = 128;

/// The deployment-provisioned SQS queue mode.
#[derive(Clone, Eq, PartialEq)]
pub enum SqsQueueKind {
    /// A Standard queue with at-least-once delivery and no ordering contract.
    Standard,
    /// A FIFO queue with an application-chosen, stable message group.
    Fifo {
        /// The group that SQS uses to preserve order within this publisher route.
        message_group_id: String,
    },
}

impl fmt::Debug for SqsQueueKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standard => formatter.write_str("Standard"),
            Self::Fifo { .. } => formatter
                .debug_struct("Fifo")
                .field("message_group_id", &"[REDACTED]")
                .finish(),
        }
    }
}

impl SqsQueueKind {
    /// Creates a FIFO queue mode with one bounded SQS message-group identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidFifoMessageGroup`] when the identifier is empty, oversized,
    /// or contains characters outside the SQS message-group character set.
    pub fn fifo(message_group_id: impl Into<String>) -> Result<Self, ConfigError> {
        let message_group_id = message_group_id.into();
        validate_fifo_identifier(&message_group_id)?;
        Ok(Self::Fifo { message_group_id })
    }

    /// Returns whether this target must be a FIFO queue.
    #[must_use]
    pub const fn is_fifo(&self) -> bool {
        matches!(self, Self::Fifo { .. })
    }

    pub(crate) fn message_group_id(&self) -> Option<&str> {
        match self {
            Self::Standard => None,
            Self::Fifo { message_group_id } => Some(message_group_id),
        }
    }
}

/// One deployment-provisioned SQS queue used as a source or destination.
#[derive(Clone, Eq, PartialEq)]
pub struct SqsQueueTarget {
    queue_url: String,
    kind: SqsQueueKind,
}

impl SqsQueueTarget {
    /// Creates a validated HTTP(S) queue URL and its expected deployment queue mode.
    ///
    /// HTTP is accepted for `LocalStack` and other explicit local test endpoints. Production AWS
    /// queue URLs use HTTPS and credentials remain entirely in the injected AWS SDK client.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidQueueUrl`] when the URL is not an absolute path-only HTTP(S)
    /// URL, has embedded credentials, or exceeds the SQS queue URL bound.
    pub fn new(queue_url: impl Into<String>, kind: SqsQueueKind) -> Result<Self, ConfigError> {
        let queue_url = queue_url.into();
        validate_queue_url(&queue_url)?;
        Ok(Self { queue_url, kind })
    }

    /// Returns the configured SQS queue URL.
    #[must_use]
    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    /// Returns the expected deployment queue mode.
    #[must_use]
    pub fn kind(&self) -> &SqsQueueKind {
        &self.kind
    }
}

impl fmt::Debug for SqsQueueTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsQueueTarget")
            .field("queue_url", &"[REDACTED]")
            .field("kind", &self.kind)
            .finish()
    }
}

fn validate_queue_url(queue_url: &str) -> Result<(), ConfigError> {
    if queue_url.is_empty()
        || queue_url.len() > MAX_QUEUE_URL_BYTES
        || queue_url.chars().any(char::is_whitespace)
    {
        return Err(ConfigError::InvalidQueueUrl);
    }
    let parsed = Url::parse(queue_url).map_err(|_| ConfigError::InvalidQueueUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path().trim_matches('/').is_empty()
    {
        return Err(ConfigError::InvalidQueueUrl);
    }
    Ok(())
}

fn validate_fifo_identifier(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > MAX_FIFO_IDENTIFIER_BYTES
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".contains(character)
        })
    {
        return Err(ConfigError::InvalidFifoMessageGroup);
    }
    Ok(())
}
