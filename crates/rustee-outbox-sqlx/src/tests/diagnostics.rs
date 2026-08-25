use std::{error::Error as StdError, fmt};

use rustee_events::EnvelopeError as EventEnvelopeError;
use rustee_jobs::EnvelopeError as JobEnvelopeError;

use crate::{
    InboxRegisterError, OutboxError, OutboxStageError, RelayError, RelayReport, ScheduleEventError,
    ScheduleJobError, StageEventError, StageJobError,
};

#[test]
fn staging_errors_redact_external_envelope_metadata_and_preserve_sources() {
    let event = StageEventError::Envelope(EventEnvelopeError::UnexpectedEventType {
        expected: "orders.paid",
        actual: "private-external-event-type".to_owned(),
    });
    let job = StageJobError::Envelope(JobEnvelopeError::UnexpectedJobName {
        expected: "receipts.send",
        actual: "private-external-job-name".to_owned(),
    });

    for error in [&event as &dyn StdError, &job as &dyn StdError] {
        assert!(!format!("{error:?}").contains("private-external"));
        assert!(!error.to_string().contains("private-external"));
        assert!(StdError::source(error).is_some());
    }
}

struct LeakyPublisherError;

impl fmt::Debug for LeakyPublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyPublisherError(private-broker-detail)")
    }
}

impl fmt::Display for LeakyPublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-broker-detail")
    }
}

impl StdError for LeakyPublisherError {}

fn leaky_sqlx_error() -> sqlx::Error {
    sqlx::Error::Protocol("private-database-detail".to_owned())
}

#[test]
fn outbox_diagnostics_redact_storage_and_publisher_details_and_preserve_sources() {
    let scheduled_job = ScheduleJobError::Database(leaky_sqlx_error());
    let scheduled_event = ScheduleEventError::Database(leaky_sqlx_error());
    let outbox = OutboxError::Database(leaky_sqlx_error());
    let stage = OutboxStageError::Database(leaky_sqlx_error());
    let inbox = InboxRegisterError::Database(leaky_sqlx_error());
    let relay = RelayError::<LeakyPublisherError>::Publisher {
        source: LeakyPublisherError,
        report: RelayReport::default(),
    };

    for error in [
        &scheduled_job as &dyn StdError,
        &scheduled_event as &dyn StdError,
        &outbox as &dyn StdError,
        &stage as &dyn StdError,
        &inbox as &dyn StdError,
        &relay as &dyn StdError,
    ] {
        assert!(!format!("{error:?}").contains("private-database-detail"));
        assert!(!format!("{error:?}").contains("private-broker-detail"));
        assert!(!error.to_string().contains("private-database-detail"));
        assert!(!error.to_string().contains("private-broker-detail"));
        assert!(StdError::source(error).is_some());
    }
}
