use rustee_events::{EventEnvelope, EventMessage, EventMessageError};
use rustee_jobs::JobEnvelope;

use crate::{
    OutboxDestination, OutboxKind, OutboxMessage, OutboxMessageError, OutboxPriority, StageOutcome,
};

use super::support::{OrderPaid, PrivateOrderPaid, SendReceipt};

#[test]
fn event_and_job_messages_preserve_their_provider_metadata() {
    let event =
        EventEnvelope::with_metadata(rustee_events::EventId::new(), OrderPaid, "account-7", 123)
            .unwrap();
    let job = JobEnvelope::with_metadata(rustee_jobs::JobId::new(), SendReceipt, 456);
    let event_message =
        OutboxMessage::event(OutboxDestination::new("orders.events").unwrap(), &event).unwrap();
    let job_message =
        OutboxMessage::job(OutboxDestination::new("jobs.receipts").unwrap(), &job).unwrap();

    assert_eq!(event_message.kind(), OutboxKind::Event);
    assert_eq!(event_message.destination().as_str(), "orders.events");
    assert_eq!(job_message.kind(), OutboxKind::Job);
    assert_eq!(job_message.destination().as_str(), "jobs.receipts");
}

#[test]
fn outbox_message_debug_output_redacts_ordering_key_and_payload() {
    let event_id = rustee_events::EventId::new();
    let event = EventEnvelope::with_metadata(
        event_id,
        PrivateOrderPaid {
            account_note: "private account note".to_owned(),
        },
        "private-ordering-key",
        123,
    )
    .unwrap();
    let message = OutboxMessage::event(
        OutboxDestination::new("private-destination").unwrap(),
        &event,
    )
    .unwrap();

    let output = format!("{message:?}");

    for sensitive in [
        "private account note",
        "account_note",
        "private-ordering-key",
        "private-destination",
        "orders.private-paid",
        &event_id.to_string(),
        &message.id().to_string(),
    ] {
        assert!(!output.contains(sensitive));
    }
    assert!(output.contains("[REDACTED]"));
}

#[test]
fn outbox_identity_debug_output_is_redacted() {
    let destination = OutboxDestination::new("private-destination").unwrap();
    let event = EventEnvelope::with_metadata(
        rustee_events::EventId::new(),
        OrderPaid,
        "private-ordering-key",
        123,
    )
    .unwrap();
    let message = OutboxMessage::event(destination.clone(), &event).unwrap();
    let id = message.id();
    let output = format!("{destination:?} {id:?} {:?}", StageOutcome::Inserted(id));

    assert!(!output.contains("private-destination"));
    assert!(!output.contains(&id.to_string()));
    assert!(output.contains("[REDACTED]"));
}

#[test]
fn outbox_messages_default_to_normal_priority_and_can_be_overridden() {
    let event =
        EventEnvelope::with_metadata(rustee_events::EventId::new(), OrderPaid, "account-7", 123)
            .unwrap();
    let message =
        OutboxMessage::event(OutboxDestination::new("orders.events").unwrap(), &event).unwrap();

    assert_eq!(message.priority(), OutboxPriority::NORMAL);
    assert_eq!(message.priority().value(), 0);

    let prioritized = message.with_priority(OutboxPriority::new(200));
    assert_eq!(prioritized.priority().value(), 200);
}

#[test]
fn outbox_revalidates_provider_metadata_for_its_durable_storage_contract() {
    assert!(matches!(
        OutboxDestination::new(" \u{0}"),
        Err(OutboxMessageError::InvalidDestination)
    ));
    assert!(matches!(
        OutboxDestination::new("x".repeat(256)),
        Err(OutboxMessageError::InvalidDestination)
    ));

    let event = EventMessage::from_parts(
        rustee_events::EventId::new(),
        "orders.paid",
        1,
        "account\0private",
        vec![b'{'],
    )
    .unwrap();
    assert!(matches!(
        OutboxMessage::from_event_message(OutboxDestination::new("orders.events").unwrap(), event),
        Err(OutboxMessageError::InvalidOrderingKey)
    ));

    assert_eq!(
        EventMessage::from_parts(
            rustee_events::EventId::new(),
            "x".repeat(256),
            1,
            "account-7",
            vec![b'{'],
        )
        .unwrap_err(),
        EventMessageError::EventTypeTooLarge
    );
}
