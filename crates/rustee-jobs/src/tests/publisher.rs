use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
};

use crate::{JobClient, JobEnvelope, JobId, JobMessage, JobPublisher, JobTraceContext};

use super::support::WelcomeEmail;

#[derive(Clone, Default)]
struct CapturingPublisher {
    messages: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl JobPublisher for CapturingPublisher {
    type Error = Infallible;

    fn publish(
        &self,
        message: JobMessage,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let messages = self.messages.clone();
        Box::pin(async move {
            messages.lock().unwrap().push(message.into_payload());
            Ok(())
        })
    }
}

#[derive(Clone)]
struct LeakyDiagnosticPublisher;

impl fmt::Debug for LeakyDiagnosticPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticPublisher(private-broker-credential)")
    }
}

impl JobPublisher for LeakyDiagnosticPublisher {
    type Error = Infallible;

    fn publish(
        &self,
        _message: JobMessage,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn typed_client_publishes_the_envelope_bytes() {
    let publisher = CapturingPublisher::default();
    let client = JobClient::new(publisher.clone());
    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
        .with_trace_context(
            JobTraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                None,
            )
            .unwrap(),
        );

    client.enqueue(&envelope).await.unwrap();
    let message = publisher.messages.lock().unwrap().pop().unwrap();
    let decoded = JobEnvelope::<WelcomeEmail>::decode(&message).unwrap();
    assert_eq!(decoded.into_payload(), WelcomeEmail { user_id: 7 });
}

#[test]
fn client_debug_does_not_delegate_to_publisher_diagnostics() {
    let client = JobClient::new(LeakyDiagnosticPublisher);

    let debug = format!("{client:?}");
    assert!(debug.contains("publisher_type"));
    assert!(!debug.contains("private-broker-credential"));
}
