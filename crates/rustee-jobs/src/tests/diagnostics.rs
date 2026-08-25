use std::{error::Error as StdError, fmt};

use crate::{CompatibleDecodeError, EnqueueError, EnvelopeError};

#[derive(Debug)]
struct LeakyError;

impl fmt::Display for LeakyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-job-error-detail")
    }
}

impl StdError for LeakyError {}

#[test]
fn job_error_diagnostics_redact_payload_and_provider_details_and_preserve_sources() {
    let envelope = EnvelopeError::UnexpectedJobName {
        expected: "email.welcome",
        actual: "private-job-name".to_owned(),
    };
    let decode = CompatibleDecodeError::Upcaster(LeakyError);
    let enqueue = EnqueueError::Provider(LeakyError);

    for error in [&envelope as &dyn StdError, &decode, &enqueue] {
        assert!(!format!("{error:?}").contains("private-job-name"));
        assert!(!format!("{error:?}").contains("private-job-error-detail"));
        assert!(!error.to_string().contains("private-job-name"));
        assert!(!error.to_string().contains("private-job-error-detail"));
    }
    assert!(StdError::source(&decode).is_some());
    assert!(StdError::source(&enqueue).is_some());
}
