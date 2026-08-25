use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    CompatibleDecodeError, DeliveryAction, EnvelopeError, Job, JobContext, JobEnvelope, JobId,
    JobRegistry, JobRegistryError, JobRegistryRegistrationError,
};

use super::support::WelcomeEmail;

#[tokio::test]
async fn registry_routes_a_typed_envelope_and_uses_the_provider_attempt() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let handler_attempts = Arc::clone(&attempts);
    let mut registry = JobRegistry::new();
    registry
        .register::<WelcomeEmail, _>(move |job: WelcomeEmail, context: JobContext| {
            let attempts = Arc::clone(&handler_attempts);
            async move {
                assert_eq!(job.user_id, 7);
                attempts.lock().unwrap().push(context.attempt());
                Ok::<_, Infallible>(())
            }
        })
        .unwrap();
    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123);

    assert_eq!(
        registry
            .dispatch(&envelope.encode().unwrap(), 3)
            .await
            .unwrap(),
        DeliveryAction::Acknowledge
    );
    assert_eq!(*attempts.lock().unwrap(), vec![3]);
    assert_eq!(
        registry.registered_names().collect::<Vec<_>>(),
        ["email.welcome"]
    );
}

#[test]
fn registry_debug_reports_only_cardinality() {
    let mut registry = JobRegistry::new();
    registry
        .register::<WelcomeEmail, _>(|_job: WelcomeEmail, _context: JobContext| async {
            Ok::<_, Infallible>(())
        })
        .unwrap();

    let debug = format!("{registry:?}");
    assert!(debug.contains("registered_job_count: 1"));
    assert!(!debug.contains("email.welcome"));
}

#[tokio::test]
async fn registry_rejects_duplicate_and_unknown_job_types_without_leaking_payload_data() {
    let mut registry = JobRegistry::new();
    registry
        .register::<WelcomeEmail, _>(|_job: WelcomeEmail, _context: JobContext| async {
            Ok::<_, Infallible>(())
        })
        .unwrap();
    assert_eq!(
        registry
            .register::<WelcomeEmail, _>(|_job: WelcomeEmail, _context: JobContext| async {
                Ok::<_, Infallible>(())
            },)
            .unwrap_err(),
        JobRegistryRegistrationError::DuplicateJobName
    );

    let unknown = JobEnvelope::with_metadata(JobId::new(), Newsletter { user_id: 8 }, 123);
    assert_eq!(
        registry.dispatch(&unknown.encode().unwrap(), 1).await,
        Err(JobRegistryError::UnknownJob)
    );
    assert_eq!(
        registry.dispatch(b"not-json", 1).await,
        Err(JobRegistryError::InvalidEnvelope)
    );
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Newsletter {
    user_id: u64,
}

impl Job for Newsletter {
    const NAME: &'static str = "email.newsletter";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WelcomeEmailV2 {
    user_id: u64,
    locale: String,
}

impl Job for WelcomeEmailV2 {
    const NAME: &'static str = "email.welcome";
    const VERSION: u16 = 2;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InvalidRegistryJob;

impl Job for InvalidRegistryJob {
    const NAME: &'static str = "invalid job";
    const VERSION: u16 = 1;
}

#[test]
fn registry_reuses_the_shared_job_name_contract() {
    let mut registry = JobRegistry::new();

    assert_eq!(
        registry
            .register::<InvalidRegistryJob, _>(|_job, _context| async { Ok::<_, Infallible>(()) })
            .unwrap_err(),
        JobRegistryRegistrationError::InvalidJobName
    );
}

#[test]
fn compatible_decode_explicitly_upcasts_only_an_older_job_payload() {
    let id = JobId::new();
    let legacy = serde_json::json!({
        "id": id,
        "name": "email.welcome",
        "version": 1,
        "payload": { "user_id": 7 },
        "idempotency_key": "welcome:7",
        "enqueued_at_unix_ms": 123,
        "attempt": 2,
    });
    let upcaster =
        |source_version, payload: Value| -> Result<WelcomeEmailV2, std::convert::Infallible> {
            assert_eq!(source_version, 1);
            Ok(WelcomeEmailV2 {
                user_id: payload["user_id"].as_u64().unwrap(),
                locale: "ko-KR".to_owned(),
            })
        };

    let decoded = JobEnvelope::<WelcomeEmailV2>::decode_compatible(
        &serde_json::to_vec(&legacy).unwrap(),
        &upcaster,
    )
    .unwrap();
    assert_eq!(decoded.id(), id);
    assert_eq!(decoded.version(), 2);
    assert_eq!(decoded.attempt(), 2);
    assert_eq!(decoded.idempotency_key(), Some("welcome:7"));
    assert_eq!(
        decoded.into_payload(),
        WelcomeEmailV2 {
            user_id: 7,
            locale: "ko-KR".to_owned(),
        }
    );
}

#[test]
fn compatible_decode_rejects_a_newer_job_before_upcasting() {
    let newer = serde_json::json!({
        "id": JobId::new(),
        "name": "email.welcome",
        "version": 3,
        "payload": { "user_id": 7, "locale": "ko-KR" },
        "idempotency_key": null,
        "enqueued_at_unix_ms": 123,
        "attempt": 1,
    });
    let upcaster =
        |_source_version, _payload: Value| -> Result<WelcomeEmailV2, std::convert::Infallible> {
            unreachable!("newer job versions must not reach the upcaster")
        };

    assert!(matches!(
        JobEnvelope::<WelcomeEmailV2>::decode_compatible(
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

#[tokio::test]
async fn registry_can_dispatch_an_explicitly_upcast_older_job_payload() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let handler_received = Arc::clone(&received);
    let mut registry = JobRegistry::new();
    registry
        .register_with_upcaster::<WelcomeEmailV2, _, _>(
            move |job: WelcomeEmailV2, context: JobContext| {
                let received = Arc::clone(&handler_received);
                async move {
                    received
                        .lock()
                        .unwrap()
                        .push((job.user_id, job.locale, context.attempt()));
                    Ok::<_, Infallible>(())
                }
            },
            |source_version, payload: Value| -> Result<WelcomeEmailV2, Infallible> {
                assert_eq!(source_version, 1);
                Ok(WelcomeEmailV2 {
                    user_id: payload["user_id"].as_u64().unwrap(),
                    locale: "ko-KR".to_owned(),
                })
            },
        )
        .unwrap();
    let legacy = serde_json::json!({
        "id": JobId::new(),
        "name": "email.welcome",
        "version": 1,
        "payload": { "user_id": 7 },
        "idempotency_key": null,
        "enqueued_at_unix_ms": 123,
        "attempt": 1,
    });

    assert_eq!(
        registry
            .dispatch(&serde_json::to_vec(&legacy).unwrap(), 3)
            .await
            .unwrap(),
        DeliveryAction::Acknowledge
    );
    assert_eq!(*received.lock().unwrap(), vec![(7, "ko-KR".to_owned(), 3)]);
}
