//! Stable internal facade for `PostgreSQL` outbox leasing.

mod error;
mod record;
mod storage;

pub use error::OutboxError;
pub use record::{LeasedEvent, LeasedJob};
pub use storage::PostgresOutbox;
