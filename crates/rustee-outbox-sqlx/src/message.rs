//! Durable outbox message facade.

mod model;
mod staging;

pub use model::{
    OutboxDestination, OutboxId, OutboxKind, OutboxMessage, OutboxMessageError, OutboxPriority,
};
pub use staging::{StageEventError, StageJobError, StageOutcome};

pub(crate) use model::validate_durable_message_fields;
