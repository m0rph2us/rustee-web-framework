use crate::{AiBatchConfigError, AiBatchReceipt, AiBatchReference, MAX_BATCH_IDENTIFIER_BYTES};

use super::support::{artifact_reference, reference};

#[test]
fn references_are_content_free_and_bounded() {
    assert!(AiBatchReference::new("tenant-a", "catalog-1", "run-1").is_ok());
    let identifier = "a".repeat(MAX_BATCH_IDENTIFIER_BYTES);
    assert!(
        AiBatchReference::new(identifier.clone(), identifier.clone(), identifier.clone(),).is_ok()
    );
    assert_eq!(
        AiBatchReference::new(
            "a".repeat(MAX_BATCH_IDENTIFIER_BYTES + 1),
            "catalog-1",
            "run-1",
        )
        .unwrap_err(),
        AiBatchConfigError::InvalidIdentifier
    );
    assert_eq!(
        AiBatchReference::new("tenant a", "catalog-1", "run-1").unwrap_err(),
        AiBatchConfigError::InvalidIdentifier
    );
    assert!(AiBatchReference::new("tenant-a", "private prompt", "run-1").is_err());
    assert!(
        serde_json::from_str::<AiBatchReference>(
            r#"{"scope":"tenant a","catalog_id":"catalog-1","run_key":"run-1"}"#,
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<AiBatchReceipt>(r#"{"provider_batch_id":"private receipt text"}"#)
            .is_err()
    );
    let receipt = AiBatchReceipt::new("private-provider-batch-id").unwrap();
    assert!(!format!("{receipt:?}").contains("private-provider-batch-id"));
}

#[test]
fn durable_reference_debug_output_redacts_submission_and_artifact_identifiers() {
    let batch = reference();
    let artifact = artifact_reference();
    let batch_output = format!("{batch:?}");
    let artifact_output = format!("{artifact:?}");

    for value in [batch.scope(), batch.catalog_id(), batch.run_key()] {
        assert!(!batch_output.contains(value));
        assert!(!artifact_output.contains(value));
    }
    for value in [artifact.provider_file_id(), artifact.reconciliation_key()] {
        assert!(!artifact_output.contains(value));
    }
}
