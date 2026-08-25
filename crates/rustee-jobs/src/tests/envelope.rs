use std::convert::Infallible;

use serde_json::{Value, json};

use crate::{
    CompatibleDecodeError, EnvelopeError, Job, JobContext, JobEnvelope, JobId, JobMessage,
    JobMessageError, JobTraceContext, MAX_JOB_ENVELOPE_BYTES, MAX_JOB_IDEMPOTENCY_KEY_BYTES,
    MAX_JOB_NAME_BYTES,
};

use super::support::{LargeJob, WelcomeEmail};

#[test]
fn envelope_round_trip_preserves_idempotency_metadata() {
    let id = JobId::new();
    let envelope = JobEnvelope::with_metadata(id, WelcomeEmail { user_id: 7 }, 123)
        .with_idempotency_key("welcome:7")
        .unwrap()
        .with_trace_context(
            JobTraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                Some("vendor=one".to_owned()),
            )
            .unwrap(),
        );

    let decoded = JobEnvelope::<WelcomeEmail>::decode(&envelope.encode().unwrap()).unwrap();
    assert_eq!(decoded.id(), id);
    assert_eq!(decoded.idempotency_key(), Some("welcome:7"));
    assert_eq!(decoded.enqueued_at_unix_ms(), 123);
    assert_eq!(decoded.attempt(), 1);
    assert_eq!(
        decoded.trace_context().map(JobTraceContext::traceparent),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
}

#[test]
fn envelope_bytes_are_bounded_for_encoding_decoding_and_relay_recovery() {
    let oversized = vec![b'x'; MAX_JOB_ENVELOPE_BYTES + 1];
    assert!(matches!(
        JobEnvelope::<WelcomeEmail>::decode(&oversized),
        Err(EnvelopeError::TooLarge)
    ));
    let upcaster =
        |_source_version, _payload: Value| -> Result<LargeJob, Infallible> { unreachable!() };
    assert!(matches!(
        JobEnvelope::<LargeJob>::decode_compatible(&oversized, &upcaster),
        Err(CompatibleDecodeError::Envelope(EnvelopeError::TooLarge))
    ));
    assert!(matches!(
        JobMessage::from_parts(JobId::new(), "email.welcome", 1, 1, oversized),
        Err(JobMessageError::PayloadTooLarge)
    ));

    let envelope = JobEnvelope::with_metadata(
        JobId::new(),
        LargeJob {
            body: "x".repeat(MAX_JOB_ENVELOPE_BYTES),
        },
        123,
    );
    assert!(matches!(envelope.encode(), Err(EnvelopeError::TooLarge)));
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct InvalidNamedJob;

impl Job for InvalidNamedJob {
    const NAME: &'static str = "invalid job";
    const VERSION: u16 = 1;
}

#[test]
fn job_name_contract_is_shared_by_envelopes_and_relay_messages() {
    assert!(matches!(
        JobEnvelope::new(InvalidNamedJob).encode(),
        Err(EnvelopeError::InvalidJobName)
    ));
    assert!(matches!(
        JobMessage::from_parts(
            JobId::new(),
            "x".repeat(MAX_JOB_NAME_BYTES + 1),
            1,
            1,
            vec![b'{']
        ),
        Err(JobMessageError::InvalidName)
    ));
    assert!(matches!(
        JobMessage::from_parts(JobId::new(), "email welcome", 1, 1, vec![b'{']),
        Err(JobMessageError::InvalidName)
    ));
    assert!(matches!(
        JobMessage::from_parts(JobId::new(), "email\0welcome", 1, 1, vec![b'{']),
        Err(JobMessageError::InvalidName)
    ));
    assert!(
        JobMessage::from_parts(
            JobId::new(),
            "x".repeat(MAX_JOB_NAME_BYTES),
            1,
            1,
            vec![b'{'],
        )
        .is_ok()
    );
}

#[test]
fn trace_context_rejects_unsafe_carrier_values() {
    assert!(matches!(
        JobTraceContext::new(" ", None),
        Err(EnvelopeError::InvalidTraceContext)
    ));
    assert!(matches!(
        JobTraceContext::new("traceparent", Some("not-ascii-\u{2603}".to_owned())),
        Err(EnvelopeError::InvalidTraceContext)
    ));
}

#[test]
fn idempotency_keys_are_bounded_during_construction_and_restoration() {
    let accepted = "k".repeat(MAX_JOB_IDEMPOTENCY_KEY_BYTES);
    assert!(
        JobEnvelope::new(WelcomeEmail { user_id: 7 })
            .with_idempotency_key(accepted)
            .is_ok()
    );

    let oversized = "k".repeat(MAX_JOB_IDEMPOTENCY_KEY_BYTES + 1);
    assert!(matches!(
        JobEnvelope::new(WelcomeEmail { user_id: 7 }).with_idempotency_key(oversized.clone()),
        Err(EnvelopeError::IdempotencyKeyTooLarge)
    ));

    let envelope = JobEnvelope::new(WelcomeEmail { user_id: 7 });
    let mut invalid = serde_json::to_value(envelope).unwrap();
    invalid["idempotency_key"] = json!(oversized);
    assert!(serde_json::from_value::<JobEnvelope<WelcomeEmail>>(invalid.clone()).is_err());
    assert!(matches!(
        JobEnvelope::<WelcomeEmail>::decode(&serde_json::to_vec(&invalid).unwrap()),
        Err(EnvelopeError::IdempotencyKeyTooLarge)
    ));
}

#[test]
fn serde_restoration_revalidates_durable_job_models() {
    assert!(
        serde_json::from_value::<JobTraceContext>(json!({
            "traceparent": " ",
            "tracestate": null,
        }))
        .is_err()
    );

    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
        .with_idempotency_key("welcome:7")
        .unwrap()
        .with_trace_context(
            JobTraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                None,
            )
            .unwrap(),
        );
    let value = serde_json::to_value(&envelope).unwrap();
    let restored = serde_json::from_value::<JobEnvelope<WelcomeEmail>>(value.clone()).unwrap();
    assert_eq!(restored, envelope);

    let mut invalid_attempt = value.clone();
    invalid_attempt["attempt"] = json!(0);
    assert!(serde_json::from_value::<JobEnvelope<WelcomeEmail>>(invalid_attempt).is_err());

    let mut invalid_name = value.clone();
    invalid_name["name"] = json!("other.job");
    assert!(serde_json::from_value::<JobEnvelope<WelcomeEmail>>(invalid_name).is_err());

    let mut invalid_trace = value;
    invalid_trace["trace_context"] = json!({
        "traceparent": "not-ascii-\u{2603}",
        "tracestate": null,
    });
    assert!(serde_json::from_value::<JobEnvelope<WelcomeEmail>>(invalid_trace).is_err());
}

#[test]
fn durable_job_debug_output_redacts_payload_keys_and_trace_context() {
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
        .with_idempotency_key("customer-7:welcome")
        .unwrap()
        .with_trace_context(JobTraceContext::new(traceparent, None).unwrap());
    let id = envelope.id().to_string();
    let message = envelope.message().unwrap();
    let context = JobContext::from_envelope(&envelope);
    let outputs = [
        format!("{envelope:?}"),
        format!("{message:?}"),
        format!("{context:?}"),
        format!("{:?}", envelope.id()),
        format!("{:?}", envelope.trace_context().unwrap()),
    ];

    for output in outputs {
        assert!(!output.contains(&id));
        assert!(!output.contains("user_id"));
        assert!(!output.contains("customer-7:welcome"));
        assert!(!output.contains(traceparent));
    }
}

#[test]
fn provider_delivery_attempt_replaces_the_stored_attempt() {
    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
        .with_attempt(3)
        .unwrap();

    assert_eq!(envelope.attempt(), 3);
    assert!(matches!(
        envelope.with_attempt(0),
        Err(EnvelopeError::InvalidAttempt)
    ));
}
