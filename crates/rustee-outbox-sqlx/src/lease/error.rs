use std::fmt;

/// Database or trusted-record failure while operating the `PostgreSQL` outbox.
#[derive(thiserror::Error)]
pub enum OutboxError {
    /// The `PostgreSQL` query failed.
    #[error("PostgreSQL outbox storage failed")]
    Database(#[from] sqlx::Error),
    /// A duration could not be represented as the bounded `PostgreSQL` interval used by this crate.
    #[error("outbox duration must be zero or at least 1 millisecond, and at most one hour")]
    InvalidDuration,
    /// A stored row did not contain a valid event message.
    #[error("stored outbox event record is invalid")]
    StoredEvent,
    /// A stored row did not contain a valid durable-job message.
    #[error("stored outbox job record is invalid")]
    StoredJob,
}

impl fmt::Debug for OutboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Database(_) => "database_failed",
            Self::InvalidDuration => "invalid_duration",
            Self::StoredEvent => "stored_event_invalid",
            Self::StoredJob => "stored_job_invalid",
        };
        formatter
            .debug_struct("OutboxError")
            .field("kind", &kind)
            .finish()
    }
}
