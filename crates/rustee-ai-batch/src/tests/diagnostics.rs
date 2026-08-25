use std::fmt;

use crate::{AiBatchArtifactReconciliationError, AiBatchSubmissionError};

struct LeakyDiagnosticError;

impl fmt::Debug for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticError(private-batch-content)")
    }
}

impl fmt::Display for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-batch-content")
    }
}

impl std::error::Error for LeakyDiagnosticError {}

#[test]
fn batch_error_debug_output_redacts_adapter_diagnostics() {
    let submission = AiBatchSubmissionError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Provider {
        source: LeakyDiagnosticError,
    };
    let artifact = AiBatchArtifactReconciliationError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Processor {
        source: LeakyDiagnosticError,
    };

    assert_eq!(
        format!("{submission:?}"),
        "AiBatchSubmissionError::Provider"
    );
    assert_eq!(
        format!("{artifact:?}"),
        "AiBatchArtifactReconciliationError::Processor"
    );

    for error in [&submission as &dyn std::error::Error, &artifact] {
        assert!(std::error::Error::source(error).is_some());
        assert!(!format!("{error:?}").contains("private-batch-content"));
        assert!(!error.to_string().contains("private-batch-content"));
    }
}
