//! Durable `PostgreSQL` inbox receipts for idempotent database-backed consumers.

use std::fmt;

use rustee_events::EventId;
use rustee_jobs::JobId;
use sqlx::{Postgres, Transaction};

const MAX_MESSAGE_ID_BYTES: usize = 255;

/// The deployment migration for the transactional consumer inbox table.
pub const INBOX_MIGRATION_SQL: &str = include_str!("../migrations/0002_rustee_inbox.sql");

/// A bounded consumer identity that scopes one durable idempotency ledger.
///
/// Use a stable name per projection, integration, or job side effect. Independent consumer groups
/// must not accidentally share a receipt merely because they receive the same event ID.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct InboxConsumer(String);

impl InboxConsumer {
    /// Creates a non-empty, bounded consumer identity.
    ///
    /// # Errors
    ///
    /// Returns [`InboxError::InvalidConsumer`] when the value is blank, contains a NUL byte, or
    /// exceeds the storage bound.
    pub fn new(consumer: impl Into<String>) -> Result<Self, InboxError> {
        let consumer = consumer.into();
        validate_inbox_text(&consumer, InboxError::InvalidConsumer)?;
        Ok(Self(consumer))
    }

    /// Returns the stable consumer identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InboxConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for InboxConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InboxConsumer([REDACTED])")
    }
}

/// A bounded source message identifier used as an inbox receipt key.
///
/// [`fmt::Debug`] redacts the identifier because it can originate from an external broker.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct InboxMessageId(String);

impl InboxMessageId {
    /// Creates a receipt key from a stable source message identifier.
    ///
    /// # Errors
    ///
    /// Returns [`InboxError::InvalidMessageId`] when the value is blank, contains a NUL byte, or
    /// exceeds the storage bound.
    pub fn new(message_id: impl Into<String>) -> Result<Self, InboxError> {
        let message_id = message_id.into();
        validate_inbox_text(&message_id, InboxError::InvalidMessageId)?;
        Ok(Self(message_id))
    }

    /// Creates a receipt key from one Rustee event ID.
    #[must_use]
    pub fn event(id: EventId) -> Self {
        Self(id.to_string())
    }

    /// Creates a receipt key from one Rustee durable job ID.
    #[must_use]
    pub fn job(id: JobId) -> Self {
        Self(id.to_string())
    }

    /// Returns the stable source message identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InboxMessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for InboxMessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InboxMessageId([REDACTED])")
    }
}

/// Result of registering a durable receipt inside the caller's side-effect transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxDecision {
    /// This transaction owns the first successful business effect for this consumer and source ID.
    FirstDelivery,
    /// A previously committed transaction already completed this consumer and source ID.
    Duplicate,
}

/// Failure while registering one durable inbox receipt.
///
/// Display and debug output retain only a safe failure category. The database source remains
/// available through [`std::error::Error::source`] for trusted transaction diagnostics.
#[derive(thiserror::Error)]
pub enum InboxRegisterError {
    /// `PostgreSQL` rejected the receipt insert or the inbox migration is unavailable.
    #[error("PostgreSQL inbox receipt registration failed")]
    Database(#[from] sqlx::Error),
}

impl fmt::Debug for InboxRegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboxRegisterError")
            .field("kind", &"database_failed")
            .finish()
    }
}

/// `PostgreSQL` inbox receipt operations for idempotent database-backed consumers.
///
/// Call [`Self::register`] in the same transaction as the database side effect. A rollback removes
/// a first-delivery receipt, while a committed receipt makes the next delivery return
/// [`InboxDecision::Duplicate`]. This is intentionally not a generic handler wrapper: external
/// side effects such as email or HTTP calls require their own idempotency capability.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresInbox;

impl PostgresInbox {
    /// Registers a source message receipt in the caller's open business-data transaction.
    ///
    /// A caller that receives [`InboxDecision::FirstDelivery`] performs only database side effects
    /// in this transaction and commits it. A caller that receives [`InboxDecision::Duplicate`]
    /// skips those side effects and can return handler success so its broker position is settled.
    ///
    /// # Errors
    ///
    /// Returns a content-free [`InboxRegisterError`] when the inbox migration is absent or the
    /// transaction fails. The database source remains available through the error chain.
    pub async fn register(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        consumer: &InboxConsumer,
        message_id: &InboxMessageId,
    ) -> Result<InboxDecision, InboxRegisterError> {
        let result = sqlx::query(
            "INSERT INTO rustee_inbox (consumer, message_id) VALUES ($1, $2) \
             ON CONFLICT (consumer, message_id) DO NOTHING",
        )
        .bind(consumer.as_str())
        .bind(message_id.as_str())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(InboxDecision::FirstDelivery)
        } else {
            Ok(InboxDecision::Duplicate)
        }
    }
}

/// Invalid inbox receipt metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InboxError {
    /// The stable consumer identity was not safe to store.
    #[error("inbox consumer must be non-blank, NUL-free, and bounded")]
    InvalidConsumer,
    /// The source message identifier was not safe to store.
    #[error("inbox message ID must be non-blank, NUL-free, and bounded")]
    InvalidMessageId,
}

fn validate_inbox_text(value: &str, error: InboxError) -> Result<(), InboxError> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > MAX_MESSAGE_ID_BYTES {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{InboxConsumer, InboxMessageId};

    #[test]
    fn message_identifier_debug_output_is_redacted() {
        let message_id = InboxMessageId::new("external-broker-message-123").unwrap();

        let output = format!("{message_id:?}");

        assert!(!output.contains(message_id.as_str()));
    }

    #[test]
    fn consumer_identity_debug_output_is_redacted() {
        let consumer = InboxConsumer::new("private-consumer-identity").unwrap();

        let output = format!("{consumer:?}");

        assert!(!output.contains(consumer.as_str()));
        assert!(output.contains("[REDACTED]"));
    }
}
