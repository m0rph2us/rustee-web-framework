//! `PostgreSQL` transactional outbox storage and inbox deduplication for `Rustee` events and jobs.
//!
//! Applications write their business rows and one [`OutboxMessage`] through the same `PostgreSQL`
//! transaction. Relays lease rows with `FOR UPDATE SKIP LOCKED`, publish through the existing
//! event or job publisher contract, then confirm the lease. A process crash or lost confirmation
//! after a broker acknowledgement can publish a message again, so consumers must remain
//! idempotent. This crate intentionally does not claim a cross-store exactly-once guarantee.
//!
//! The SQL migrations are deployment-owned. Add [`OUTBOX_MIGRATION_SQL`] and
//! [`INBOX_MIGRATION_SQL`] to an application's normal migration set; do not apply them from HTTP
//! application startup.

mod inbox;
mod lease;
mod message;
mod relay;
mod stage;
mod timing;

pub use inbox::{
    INBOX_MIGRATION_SQL, InboxConsumer, InboxDecision, InboxError, InboxMessageId,
    InboxRegisterError, PostgresInbox,
};
pub use lease::{LeasedEvent, LeasedJob, OutboxError, PostgresOutbox};
pub use message::{
    OutboxDestination, OutboxId, OutboxKind, OutboxMessage, OutboxMessageError, OutboxPriority,
    StageEventError, StageJobError, StageOutcome,
};
pub use relay::{
    EventOutboxRelay, JobOutboxRelay, NoopOutboxRelayObserver, OutboxRelayObserver, RelayConfig,
    RelayConfigError, RelayError, RelayLoopConfig, RelayLoopConfigError, RelayLoopReport,
    RelayPassFinished, RelayPassKind, RelayPassObservation, RelayPassOutcome, RelayPassStarted,
    RelayReport,
};
pub use stage::OutboxStageError;
pub use timing::{
    EventSchedule, EventScheduleError, JobSchedule, JobScheduleError, LeaseConfig,
    LeaseConfigError, LeaseOutcome, ScheduleEventError, ScheduleJobError,
};

use timing::{MAX_LEASE_DURATION, MIN_POSTGRES_INTERVAL};

/// The deployment migration for the shared `PostgreSQL` outbox table.
pub const OUTBOX_MIGRATION_SQL: &str = include_str!("../migrations/0001_rustee_outbox.sql");

/// The forward-only deployment migration that adds durable local relay priority to the outbox.
///
/// Apply this after [`OUTBOX_MIGRATION_SQL`]. Existing deployments must retain their original
/// `0001` migration checksum and add this file as a new migration rather than editing history.
pub const OUTBOX_PRIORITY_MIGRATION_SQL: &str =
    include_str!("../migrations/0003_rustee_outbox_priority.sql");

#[cfg(test)]
mod tests;
