use std::fmt;

use rustee_events::{EventId, EventMessage};
use rustee_jobs::{JobId, JobMessage};
use uuid::Uuid;

use crate::{OutboxDestination, OutboxId, message::validate_durable_message_fields};

use super::error::OutboxError;

/// One leased event plus the opaque token required to settle its outbox state.
#[derive(Clone)]
pub struct LeasedEvent {
    lease: Lease,
    destination: OutboxDestination,
    message: EventMessage,
}

impl LeasedEvent {
    pub(super) fn try_from_record(record: StoredLease) -> Result<Self, OutboxError> {
        validate_durable_message_fields(
            &record.message_id,
            &record.message_type,
            &record.ordering_key,
            record.delivery_attempt,
            &record.payload,
        )
        .map_err(|_| OutboxError::StoredEvent)?;
        let message_id =
            Uuid::parse_str(&record.message_id).map_err(|_| OutboxError::StoredEvent)?;
        let message = EventMessage::from_parts(
            EventId::from_uuid(message_id),
            record.message_type,
            record.schema_version,
            record.ordering_key,
            record.payload,
        )
        .map_err(|_| OutboxError::StoredEvent)?;
        Ok(Self {
            lease: record.lease,
            destination: record.destination,
            message,
        })
    }

    pub(super) const fn lease(&self) -> &Lease {
        &self.lease
    }

    /// Returns the durable outbox row identifier.
    #[must_use]
    pub const fn id(&self) -> OutboxId {
        self.lease.id
    }

    /// Returns how many relay publish attempts have claimed this row, starting at one.
    #[must_use]
    pub const fn relay_attempt(&self) -> u32 {
        self.lease.relay_attempt
    }

    /// Returns the destination label selected for this relay.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Returns the reconstructed event provider message.
    #[must_use]
    pub fn message(&self) -> &EventMessage {
        &self.message
    }
}

impl fmt::Debug for LeasedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeasedEvent")
            .field("id", &"[REDACTED]")
            .field("destination", &"[REDACTED]")
            .field("relay_attempt", &self.lease.relay_attempt)
            .field("message", &"[REDACTED]")
            .finish()
    }
}

/// One leased durable job plus the opaque token required to settle its outbox state.
#[derive(Clone)]
pub struct LeasedJob {
    lease: Lease,
    destination: OutboxDestination,
    message: JobMessage,
}

impl LeasedJob {
    pub(super) fn try_from_record(record: StoredLease) -> Result<Self, OutboxError> {
        validate_durable_message_fields(
            &record.message_id,
            &record.message_type,
            &record.ordering_key,
            record.delivery_attempt,
            &record.payload,
        )
        .map_err(|_| OutboxError::StoredJob)?;
        let message_id = Uuid::parse_str(&record.message_id).map_err(|_| OutboxError::StoredJob)?;
        let message = JobMessage::from_parts(
            JobId::from_uuid(message_id),
            record.message_type,
            record.schema_version,
            record.delivery_attempt,
            record.payload,
        )
        .map_err(|_| OutboxError::StoredJob)?;
        Ok(Self {
            lease: record.lease,
            destination: record.destination,
            message,
        })
    }

    pub(super) const fn lease(&self) -> &Lease {
        &self.lease
    }

    /// Returns the durable outbox row identifier.
    #[must_use]
    pub const fn id(&self) -> OutboxId {
        self.lease.id
    }

    /// Returns how many relay publish attempts have claimed this row, starting at one.
    #[must_use]
    pub const fn relay_attempt(&self) -> u32 {
        self.lease.relay_attempt
    }

    /// Returns the destination label selected for this relay.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Returns the reconstructed durable job provider message.
    #[must_use]
    pub fn message(&self) -> &JobMessage {
        &self.message
    }
}

impl fmt::Debug for LeasedJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeasedJob")
            .field("id", &"[REDACTED]")
            .field("destination", &"[REDACTED]")
            .field("relay_attempt", &self.lease.relay_attempt)
            .field("message", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct Lease {
    pub(super) id: OutboxId,
    pub(super) token: Uuid,
    pub(super) relay_attempt: u32,
}

#[derive(Clone)]
pub(super) struct StoredLease {
    pub(super) lease: Lease,
    pub(super) destination: OutboxDestination,
    pub(super) message_id: String,
    pub(super) message_type: String,
    pub(super) schema_version: u16,
    pub(super) ordering_key: String,
    pub(super) delivery_attempt: u16,
    pub(super) payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::OutboxDestination;

    #[test]
    fn leased_message_debug_output_redacts_settlement_and_routing_metadata() {
        let event_row_id = OutboxId::new();
        let job_row_id = OutboxId::new();
        let event_lease_token = Uuid::new_v4();
        let job_lease_token = Uuid::new_v4();
        let event_message_id = Uuid::new_v4();
        let job_message_id = Uuid::new_v4();
        let event = LeasedEvent::try_from_record(StoredLease {
            lease: Lease {
                id: event_row_id,
                token: event_lease_token,
                relay_attempt: 3,
            },
            destination: OutboxDestination::new("private-event-destination").unwrap(),
            message_id: event_message_id.to_string(),
            message_type: "private-event-type".to_owned(),
            schema_version: 7,
            ordering_key: "private-event-key".to_owned(),
            delivery_attempt: 1,
            payload: b"private-event-payload".to_vec(),
        })
        .unwrap();
        let job = LeasedJob::try_from_record(StoredLease {
            lease: Lease {
                id: job_row_id,
                token: job_lease_token,
                relay_attempt: 4,
            },
            destination: OutboxDestination::new("private-job-destination").unwrap(),
            message_id: job_message_id.to_string(),
            message_type: "private-job-type".to_owned(),
            schema_version: 9,
            ordering_key: job_message_id.to_string(),
            delivery_attempt: 2,
            payload: b"private-job-payload".to_vec(),
        })
        .unwrap();

        let output = format!("{event:?} {job:?}");

        for sensitive in [
            &event_row_id.to_string(),
            &job_row_id.to_string(),
            &event_lease_token.to_string(),
            &job_lease_token.to_string(),
            &event_message_id.to_string(),
            &job_message_id.to_string(),
            "private-event-destination",
            "private-job-destination",
            "private-event-type",
            "private-job-type",
            "private-event-key",
            "private-event-payload",
            "private-job-payload",
        ] {
            assert!(!output.contains(sensitive));
        }
        assert!(output.contains("relay_attempt: 3"));
        assert!(output.contains("relay_attempt: 4"));
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn leased_records_revalidate_the_durable_outbox_contract() {
        let mut oversized_event_type = valid_stored_lease();
        oversized_event_type.message_type = "t".repeat(256);
        assert!(matches!(
            LeasedEvent::try_from_record(oversized_event_type),
            Err(OutboxError::StoredEvent)
        ));

        let mut oversized_event_key = valid_stored_lease();
        oversized_event_key.ordering_key = "k".repeat(513);
        assert!(matches!(
            LeasedEvent::try_from_record(oversized_event_key),
            Err(OutboxError::StoredEvent)
        ));

        let mut zero_event_attempt = valid_stored_lease();
        zero_event_attempt.delivery_attempt = 0;
        assert!(matches!(
            LeasedEvent::try_from_record(zero_event_attempt),
            Err(OutboxError::StoredEvent)
        ));

        let mut missing_job_ordering_key = valid_stored_lease();
        missing_job_ordering_key.ordering_key.clear();
        assert!(matches!(
            LeasedJob::try_from_record(missing_job_ordering_key),
            Err(OutboxError::StoredJob)
        ));
    }

    fn valid_stored_lease() -> StoredLease {
        let message_id = Uuid::new_v4();
        StoredLease {
            lease: Lease {
                id: OutboxId::new(),
                token: Uuid::new_v4(),
                relay_attempt: 1,
            },
            destination: OutboxDestination::new("orders.events").unwrap(),
            message_id: message_id.to_string(),
            message_type: "orders.paid".to_owned(),
            schema_version: 1,
            ordering_key: message_id.to_string(),
            delivery_attempt: 1,
            payload: b"{}".to_vec(),
        }
    }
}
