use std::sync::{Arc, Mutex};

use crate::{
    AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactReconciler,
    AiBatchArtifactReconciliationDisposition, AiBatchArtifactReconciliationError,
    AiBatchArtifactReference, AiBatchArtifactReservation, InMemoryAiBatchArtifactLedger,
};

use super::support::{ArtifactLoader, ArtifactProcessor, artifact_reference, reference};

#[tokio::test]
async fn artifact_reconciliation_keeps_raw_content_ephemeral_and_does_not_repeat_completion() {
    let loader_calls = Arc::new(Mutex::new(0));
    let processor_calls = Arc::new(Mutex::new(0));
    let reconciler = AiBatchArtifactReconciler::new(
        InMemoryAiBatchArtifactLedger::new(4).unwrap(),
        ArtifactLoader {
            calls: loader_calls.clone(),
        },
        ArtifactProcessor {
            calls: processor_calls.clone(),
            fail: false,
        },
    );

    let encoded = serde_json::to_string(&artifact_reference()).unwrap();
    let first = reconciler.reconcile(artifact_reference()).await.unwrap();
    let second = reconciler.reconcile(artifact_reference()).await.unwrap();

    assert!(!encoded.contains("private provider batch output body"));
    assert_eq!(first, AiBatchArtifactReconciliationDisposition::Reconciled);
    assert_eq!(
        second,
        AiBatchArtifactReconciliationDisposition::ExistingReconciliation
    );
    assert_eq!(*loader_calls.lock().unwrap(), 1);
    assert_eq!(*processor_calls.lock().unwrap(), 1);
    assert!(!format!("{reconciler:?}").contains("private provider batch output body"));
}

#[tokio::test]
async fn artifact_processor_failure_stays_pending_without_automatic_replay() {
    let loader_calls = Arc::new(Mutex::new(0));
    let processor_calls = Arc::new(Mutex::new(0));
    let reconciler = AiBatchArtifactReconciler::new(
        InMemoryAiBatchArtifactLedger::new(4).unwrap(),
        ArtifactLoader {
            calls: loader_calls.clone(),
        },
        ArtifactProcessor {
            calls: processor_calls.clone(),
            fail: true,
        },
    );

    let first = reconciler
        .reconcile(artifact_reference())
        .await
        .unwrap_err();
    let second = reconciler
        .reconcile(artifact_reference())
        .await
        .unwrap_err();

    assert!(matches!(
        first,
        AiBatchArtifactReconciliationError::Processor { .. }
    ));
    assert!(matches!(
        second,
        AiBatchArtifactReconciliationError::Pending { .. }
    ));
    assert_eq!(*loader_calls.lock().unwrap(), 1);
    assert_eq!(*processor_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn reviewed_artifact_retry_requires_a_new_reconciliation_key() {
    let ledger = InMemoryAiBatchArtifactLedger::new(4).unwrap();
    let first = artifact_reference();
    let retry = AiBatchArtifactReference::new(
        reference(),
        AiBatchArtifactKind::Output,
        "file-output-17",
        "reconcile-output-17-retry-1",
    )
    .unwrap();

    assert_eq!(
        ledger.reserve(first.clone()).await.unwrap(),
        AiBatchArtifactReservation::Reserved
    );
    assert_eq!(
        ledger.reserve(first).await.unwrap(),
        AiBatchArtifactReservation::Pending
    );
    assert_eq!(
        ledger.reserve(retry).await.unwrap(),
        AiBatchArtifactReservation::Reserved
    );
}
