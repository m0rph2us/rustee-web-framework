use std::{convert::Infallible, fmt};

use crate::{EventClient, EventMessage, EventPublisher};

#[derive(Clone)]
struct LeakyDiagnosticPublisher;

impl fmt::Debug for LeakyDiagnosticPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticPublisher(private-broker-credential)")
    }
}

impl EventPublisher for LeakyDiagnosticPublisher {
    type Error = Infallible;

    fn publish(
        &self,
        _: EventMessage,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn client_debug_does_not_delegate_to_publisher_diagnostics() {
    let client = EventClient::new(LeakyDiagnosticPublisher);

    let debug = format!("{client:?}");
    assert!(debug.contains("publisher_type"));
    assert!(!debug.contains("private-broker-credential"));
}
