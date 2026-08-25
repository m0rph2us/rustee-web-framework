use std::convert::Infallible;

use serde_json::{Value, json};

use crate::{CompatibleDecodeError, EnvelopeError, EventEnvelope, EventId};

use super::support::OrderPaidV2;

#[test]
fn compatible_decode_explicitly_upcasts_only_an_older_payload() {
    let id = EventId::new();
    let legacy = json!({
        "id": id,
        "event_type": "orders.paid",
        "version": 1,
        "key": "acct-7",
        "payload": { "order_id": 7 },
        "occurred_at_unix_ms": 123,
        "correlation_id": "request-7",
        "causation_id": null,
        "trace_context": null
    });
    let upcaster = |source_version, payload: Value| -> Result<OrderPaidV2, Infallible> {
        assert_eq!(source_version, 1);
        Ok(OrderPaidV2 {
            order_id: payload["order_id"].as_u64().unwrap(),
            currency: "KRW".to_owned(),
        })
    };

    let decoded = EventEnvelope::<OrderPaidV2>::decode_compatible(
        &serde_json::to_vec(&legacy).unwrap(),
        &upcaster,
    )
    .unwrap();
    assert_eq!(decoded.id(), id);
    assert_eq!(decoded.key(), "acct-7");
    assert_eq!(decoded.correlation_id(), Some("request-7"));
    assert_eq!(
        decoded.into_payload(),
        OrderPaidV2 {
            order_id: 7,
            currency: "KRW".to_owned(),
        }
    );
}

#[test]
fn compatible_decode_rejects_a_newer_producer_version_before_upcasting() {
    let newer = json!({
        "id": EventId::new(),
        "event_type": "orders.paid",
        "version": 3,
        "key": "acct-7",
        "payload": { "order_id": 7, "currency": "KRW" },
        "occurred_at_unix_ms": 123,
        "correlation_id": null,
        "causation_id": null,
        "trace_context": null
    });
    let upcaster = |_source_version, _payload: Value| -> Result<OrderPaidV2, Infallible> {
        unreachable!("newer versions must not reach the upcaster")
    };

    assert!(matches!(
        EventEnvelope::<OrderPaidV2>::decode_compatible(
            &serde_json::to_vec(&newer).unwrap(),
            &upcaster,
        ),
        Err(CompatibleDecodeError::Envelope(
            EnvelopeError::UnsupportedVersion {
                expected: 2,
                actual: 3,
            }
        ))
    ));
}
