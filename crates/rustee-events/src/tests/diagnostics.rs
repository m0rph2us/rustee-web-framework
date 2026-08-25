use std::{error::Error as StdError, fmt};

use crate::{CompatibleDecodeError, EnvelopeError, PublishError};

#[derive(Debug)]
struct LeakyError;

impl fmt::Display for LeakyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-event-error-detail")
    }
}

impl StdError for LeakyError {}

#[test]
fn event_error_diagnostics_redact_payload_and_provider_details_and_preserve_sources() {
    let envelope = EnvelopeError::UnexpectedEventType {
        expected: "orders.paid",
        actual: "private-event-type".to_owned(),
    };
    let decode = CompatibleDecodeError::Upcaster(LeakyError);
    let publish = PublishError::Provider(LeakyError);

    for error in [&envelope as &dyn StdError, &decode, &publish] {
        assert!(!format!("{error:?}").contains("private-event-type"));
        assert!(!format!("{error:?}").contains("private-event-error-detail"));
        assert!(!error.to_string().contains("private-event-type"));
        assert!(!error.to_string().contains("private-event-error-detail"));
    }
    assert!(StdError::source(&decode).is_some());
    assert!(StdError::source(&publish).is_some());
}
