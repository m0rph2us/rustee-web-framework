use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_jobs::{
    DeliveryAction, Job, JobClient, JobContext, JobEnvelope, JobMessage, JobPublisher, JobRegistry,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SendWelcomeEmail {
    user_id: u64,
}

impl Job for SendWelcomeEmail {
    const NAME: &'static str = "email.welcome";
    const VERSION: u16 = 1;
}

#[derive(Clone, Default)]
struct InMemoryPublisher {
    messages: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl InMemoryPublisher {
    fn take_payload(&self) -> Option<Vec<u8>> {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
    }
}

impl JobPublisher for InMemoryPublisher {
    type Error = Infallible;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let messages = Arc::clone(&self.messages);
        let payload = message.into_payload();
        Box::pin(async move {
            messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(payload);
            Ok(())
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HandledWelcomeEmail {
    user_id: u64,
    idempotency_key: Option<String>,
    attempt: u16,
}

fn registry(handled: Arc<Mutex<Vec<HandledWelcomeEmail>>>) -> JobRegistry {
    let mut registry = JobRegistry::new();
    registry
        .register::<SendWelcomeEmail, _>(move |job: SendWelcomeEmail, context: JobContext| {
            let handled = Arc::clone(&handled);
            async move {
                handled
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(HandledWelcomeEmail {
                        user_id: job.user_id,
                        idempotency_key: context.idempotency_key().map(ToOwned::to_owned),
                        attempt: context.attempt(),
                    });
                Ok::<_, Infallible>(())
            }
        })
        .expect("the example job name is static and valid");
    registry
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let publisher = InMemoryPublisher::default();
    let envelope = JobEnvelope::new(SendWelcomeEmail { user_id: 7 })
        .with_idempotency_key("welcome-email:7")?;
    JobClient::new(publisher.clone()).enqueue(&envelope).await?;

    let handled = Arc::new(Mutex::new(Vec::new()));
    let action = registry(Arc::clone(&handled))
        .dispatch(
            &publisher
                .take_payload()
                .expect("the acknowledged local publisher retained one payload"),
            1,
        )
        .await?;
    assert_eq!(action, DeliveryAction::Acknowledge);
    println!(
        "processed {} typed job",
        handled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rustee_jobs::{DeliveryAction, JobClient, JobEnvelope};

    use super::{HandledWelcomeEmail, InMemoryPublisher, SendWelcomeEmail, registry};

    #[tokio::test]
    async fn typed_envelope_is_published_then_dispatched_with_delivery_metadata() {
        let publisher = InMemoryPublisher::default();
        let envelope = JobEnvelope::new(SendWelcomeEmail { user_id: 7 })
            .with_idempotency_key("welcome-email:7")
            .unwrap();
        JobClient::new(publisher.clone())
            .enqueue(&envelope)
            .await
            .unwrap();

        let handled = Arc::new(Mutex::new(Vec::new()));
        let action = registry(Arc::clone(&handled))
            .dispatch(&publisher.take_payload().unwrap(), 2)
            .await
            .unwrap();

        assert_eq!(action, DeliveryAction::Acknowledge);
        assert_eq!(
            handled
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            [HandledWelcomeEmail {
                user_id: 7,
                idempotency_key: Some("welcome-email:7".to_owned()),
                attempt: 2,
            }]
        );
    }
}
