use std::sync::{Arc, Mutex};

use crate::{
    AiBatchSubmissionDisposition, AiBatchSubmissionError, AiBatchSubmitter, InMemoryAiBatchLedger,
};

use super::support::{Catalog, Provider, reference};

#[tokio::test]
async fn accepted_submission_records_a_receipt_and_later_delivery_does_not_resubmit() {
    let catalog_calls = Arc::new(Mutex::new(0));
    let provider_calls = Arc::new(Mutex::new(0));
    let submitter = AiBatchSubmitter::new(
        Catalog {
            calls: catalog_calls.clone(),
        },
        InMemoryAiBatchLedger::new(4).unwrap(),
        Provider {
            calls: provider_calls.clone(),
            fail: false,
        },
    );

    let first = submitter.submit(reference()).await.unwrap();
    let second = submitter.submit(reference()).await.unwrap();

    assert_eq!(first.disposition(), AiBatchSubmissionDisposition::Submitted);
    assert_eq!(
        second.disposition(),
        AiBatchSubmissionDisposition::ExistingSubmission
    );
    assert_eq!(second.receipt().provider_batch_id(), "provider-batch-42");
    assert_eq!(*catalog_calls.lock().unwrap(), 1);
    assert_eq!(*provider_calls.lock().unwrap(), 1);
    assert!(!format!("{submitter:?}").contains("private batch prompt"));
}

#[tokio::test]
async fn provider_failure_stays_pending_without_an_automatic_second_submission() {
    let provider_calls = Arc::new(Mutex::new(0));
    let submitter = AiBatchSubmitter::new(
        Catalog {
            calls: Arc::new(Mutex::new(0)),
        },
        InMemoryAiBatchLedger::new(4).unwrap(),
        Provider {
            calls: provider_calls.clone(),
            fail: true,
        },
    );

    let first = submitter.submit(reference()).await.unwrap_err();
    let second = submitter.submit(reference()).await.unwrap_err();

    assert!(matches!(first, AiBatchSubmissionError::Provider { .. }));
    assert!(matches!(second, AiBatchSubmissionError::Pending { .. }));
    assert_eq!(*provider_calls.lock().unwrap(), 1);
}
