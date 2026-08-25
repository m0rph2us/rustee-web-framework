use std::convert::Infallible;

use serde_json::{Value, json};

use crate::{
    CompatibleDecodeError, EnvelopeError, Event, EventContext, EventEnvelope, EventId,
    EventMessage, EventMessageError, EventTraceContext, MAX_EVENT_ENVELOPE_BYTES,
    MAX_EVENT_METADATA_ID_BYTES, MAX_EVENT_PARTITION_KEY_BYTES, MAX_EVENT_TYPE_BYTES,
    is_valid_event_type,
};

use super::support::{LargeEvent, OrderPaid};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct BlankTypeEvent;

impl Event for BlankTypeEvent {
    const TYPE: &'static str = " ";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct NulTypeEvent;

impl Event for NulTypeEvent {
    const TYPE: &'static str = "orders\0paid";
    const VERSION: u16 = 1;
}

#[test]
fn envelope_round_trip_preserves_key_and_correlation() {
    let envelope =
        EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "acct-1", 123)
            .unwrap()
            .with_correlation_id("request-1")
            .unwrap();

    let decoded = EventEnvelope::<OrderPaid>::decode(&envelope.encode().unwrap()).unwrap();
    assert_eq!(decoded.key(), "acct-1");
    assert_eq!(decoded.correlation_id(), Some("request-1"));
    assert_eq!(decoded.into_payload(), OrderPaid { order_id: 7 });
}

#[test]
fn envelope_bytes_are_bounded_for_encoding_decoding_and_relay_recovery() {
    let oversized = vec![b'x'; MAX_EVENT_ENVELOPE_BYTES + 1];
    assert!(matches!(
        EventEnvelope::<OrderPaid>::decode(&oversized),
        Err(EnvelopeError::TooLarge)
    ));
    let upcaster =
        |_source_version, _payload: Value| -> Result<LargeEvent, Infallible> { unreachable!() };
    assert!(matches!(
        EventEnvelope::<LargeEvent>::decode_compatible(&oversized, &upcaster),
        Err(CompatibleDecodeError::Envelope(EnvelopeError::TooLarge))
    ));
    assert!(matches!(
        EventMessage::from_parts(EventId::new(), "orders.paid", 1, "acct-1", oversized),
        Err(EventMessageError::PayloadTooLarge)
    ));

    let envelope = EventEnvelope::with_metadata(
        EventId::new(),
        LargeEvent {
            body: "x".repeat(MAX_EVENT_ENVELOPE_BYTES),
        },
        "acct-1",
        123,
    )
    .unwrap();
    assert!(matches!(envelope.encode(), Err(EnvelopeError::TooLarge)));
}

#[test]
fn blank_partition_key_is_rejected() {
    assert!(EventEnvelope::new(OrderPaid { order_id: 7 }, " ").is_err());
}

#[test]
fn metadata_identifiers_are_bounded_during_construction_and_restoration() {
    let accepted = "m".repeat(MAX_EVENT_METADATA_ID_BYTES);
    assert!(
        EventEnvelope::new(OrderPaid { order_id: 7 }, "acct-1")
            .unwrap()
            .with_correlation_id(accepted.clone())
            .unwrap()
            .with_causation_id(accepted)
            .is_ok()
    );

    let oversized = "m".repeat(MAX_EVENT_METADATA_ID_BYTES + 1);
    assert!(matches!(
        EventEnvelope::new(OrderPaid { order_id: 7 }, "acct-1")
            .unwrap()
            .with_correlation_id(oversized.clone()),
        Err(EnvelopeError::CorrelationIdTooLarge)
    ));
    assert!(matches!(
        EventEnvelope::new(OrderPaid { order_id: 7 }, "acct-1")
            .unwrap()
            .with_causation_id(oversized.clone()),
        Err(EnvelopeError::CausationIdTooLarge)
    ));

    let envelope = EventEnvelope::new(OrderPaid { order_id: 7 }, "acct-1").unwrap();
    let value = serde_json::to_value(envelope).unwrap();
    let mut oversized_correlation = value.clone();
    oversized_correlation["correlation_id"] = json!(oversized.clone());
    assert!(
        serde_json::from_value::<EventEnvelope<OrderPaid>>(oversized_correlation.clone()).is_err()
    );
    assert!(matches!(
        EventEnvelope::<OrderPaid>::decode(&serde_json::to_vec(&oversized_correlation).unwrap()),
        Err(EnvelopeError::CorrelationIdTooLarge)
    ));

    let mut oversized_causation = value;
    oversized_causation["causation_id"] = json!(oversized);
    assert!(
        serde_json::from_value::<EventEnvelope<OrderPaid>>(oversized_causation.clone()).is_err()
    );
    assert!(matches!(
        EventEnvelope::<OrderPaid>::decode(&serde_json::to_vec(&oversized_causation).unwrap()),
        Err(EnvelopeError::CausationIdTooLarge)
    ));
}

#[test]
fn partition_key_has_one_shared_envelope_and_message_bound() {
    let oversized_key = "k".repeat(MAX_EVENT_PARTITION_KEY_BYTES + 1);
    assert!(matches!(
        EventEnvelope::new(OrderPaid { order_id: 7 }, &oversized_key),
        Err(EnvelopeError::KeyTooLarge)
    ));
    assert_eq!(
        EventMessage::from_parts(EventId::new(), "orders.paid", 1, oversized_key, vec![b'x'],)
            .unwrap_err(),
        EventMessageError::KeyTooLarge
    );

    assert!(
        EventMessage::from_parts(
            EventId::new(),
            "orders.paid",
            1,
            "k".repeat(MAX_EVENT_PARTITION_KEY_BYTES),
            vec![b'x'],
        )
        .is_ok()
    );
}

#[test]
fn event_type_is_durable_safe_and_has_one_shared_envelope_and_message_bound() {
    assert!(matches!(
        EventEnvelope::new(BlankTypeEvent, "account-7"),
        Err(EnvelopeError::InvalidEventType)
    ));
    assert!(matches!(
        EventEnvelope::new(NulTypeEvent, "account-7"),
        Err(EnvelopeError::InvalidEventType)
    ));
    assert!(!is_valid_event_type("orders\0paid"));

    let oversized_type = "event".repeat(MAX_EVENT_TYPE_BYTES / 5 + 1);
    assert_eq!(
        EventMessage::from_parts(EventId::new(), oversized_type, 1, "account-7", vec![b'x'],)
            .unwrap_err(),
        EventMessageError::EventTypeTooLarge
    );
    assert!(
        EventMessage::from_parts(
            EventId::new(),
            "event".repeat(MAX_EVENT_TYPE_BYTES / 5),
            1,
            "account-7",
            vec![b'x'],
        )
        .is_ok()
    );
    assert_eq!(
        EventMessage::from_parts(EventId::new(), "orders\0paid", 1, "account-7", vec![b'x'],)
            .unwrap_err(),
        EventMessageError::InvalidEventType
    );
}

#[test]
fn trace_context_round_trip_preserves_the_bounded_carrier() {
    let envelope = EventEnvelope::new(OrderPaid { order_id: 7 }, "7")
        .unwrap()
        .with_trace_context(
            EventTraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                Some("vendor=one".to_owned()),
            )
            .unwrap(),
        );
    let decoded = EventEnvelope::<OrderPaid>::decode(&envelope.encode().unwrap()).unwrap();
    assert_eq!(
        decoded.trace_context().unwrap().traceparent(),
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    );
    assert_eq!(
        decoded.trace_context().unwrap().tracestate(),
        Some("vendor=one")
    );
}

#[test]
fn serde_restoration_revalidates_durable_event_models() {
    assert!(
        serde_json::from_value::<EventTraceContext>(json!({
            "traceparent": " ",
            "tracestate": null,
        }))
        .is_err()
    );

    let envelope =
        EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "acct-1", 123)
            .unwrap()
            .with_correlation_id("request-1")
            .unwrap()
            .with_trace_context(
                EventTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    None,
                )
                .unwrap(),
            );
    let value = serde_json::to_value(&envelope).unwrap();
    let restored = serde_json::from_value::<EventEnvelope<OrderPaid>>(value.clone()).unwrap();
    assert_eq!(restored, envelope);

    let mut invalid_key = value.clone();
    invalid_key["key"] = json!(" ");
    assert!(serde_json::from_value::<EventEnvelope<OrderPaid>>(invalid_key).is_err());

    let mut oversized_key = value.clone();
    oversized_key["key"] = json!("k".repeat(MAX_EVENT_PARTITION_KEY_BYTES + 1));
    assert!(serde_json::from_value::<EventEnvelope<OrderPaid>>(oversized_key).is_err());

    let mut invalid_version = value.clone();
    invalid_version["version"] = json!(2);
    assert!(serde_json::from_value::<EventEnvelope<OrderPaid>>(invalid_version).is_err());

    let mut invalid_trace = value;
    invalid_trace["trace_context"] = json!({
        "traceparent": "not-ascii-\u{2603}",
        "tracestate": null,
    });
    assert!(serde_json::from_value::<EventEnvelope<OrderPaid>>(invalid_trace).is_err());
}

#[test]
fn durable_event_debug_output_redacts_payload_keys_and_trace_context() {
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let envelope =
        EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "account-7", 123)
            .unwrap()
            .with_correlation_id("request-7")
            .unwrap()
            .with_causation_id("command-7")
            .unwrap()
            .with_trace_context(EventTraceContext::new(traceparent, None).unwrap());
    let id = envelope.id().to_string();
    let message = envelope.message().unwrap();
    let context = EventContext::from_envelope(&envelope);
    let outputs = [
        format!("{envelope:?}"),
        format!("{message:?}"),
        format!("{context:?}"),
        format!("{:?}", envelope.id()),
        format!("{:?}", envelope.trace_context().unwrap()),
    ];

    for output in outputs {
        assert!(!output.contains(&id));
        assert!(!output.contains("order_id"));
        assert!(!output.contains("account-7"));
        assert!(!output.contains("request-7"));
        assert!(!output.contains("command-7"));
        assert!(!output.contains(traceparent));
    }
}

#[test]
fn invalid_trace_context_is_rejected_before_or_during_decoding() {
    assert!(matches!(
        EventTraceContext::new(" ", None),
        Err(EnvelopeError::InvalidTraceContext)
    ));
    let invalid = r#"{
        "id":"550e8400-e29b-41d4-a716-446655440000",
        "event_type":"orders.paid",
        "version":1,
        "key":"7",
        "payload":{"order_id":7},
        "occurred_at_unix_ms":123,
        "correlation_id":null,
        "causation_id":null,
        "trace_context":{"traceparent":" ","tracestate":null}
    }"#;
    assert!(matches!(
        EventEnvelope::<OrderPaid>::decode(invalid.as_bytes()),
        Err(EnvelopeError::InvalidTraceContext)
    ));
}
