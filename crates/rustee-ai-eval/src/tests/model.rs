//! Evaluation model validation and content-redaction regression coverage.

use super::*;

#[test]
fn suite_rejects_duplicate_or_unsafe_identifiers() {
    let duplicate = AiEvaluationSuite::new(
        "support.v1",
        [
            AiEvaluationCase::new("answer.1", request("one"), "one".to_owned()).unwrap(),
            AiEvaluationCase::new("answer.1", request("two"), "two".to_owned()).unwrap(),
        ],
    )
    .unwrap_err();
    assert_eq!(duplicate, AiEvaluationConfigError::DuplicateCaseIdentifier);

    let invalid =
        AiEvaluationCase::new("answer one", request("one"), "one".to_owned()).unwrap_err();
    assert_eq!(invalid, AiEvaluationConfigError::InvalidIdentifier);

    let identifier = "a".repeat(MAX_EVALUATION_IDENTIFIER_BYTES);
    assert!(
        AiEvaluationReference::new(identifier.clone(), identifier.clone(), identifier,).is_ok()
    );
    assert_eq!(
        AiEvaluationReference::new(
            "a".repeat(MAX_EVALUATION_IDENTIFIER_BYTES + 1),
            "catalog-1",
            "run-1",
        )
        .unwrap_err(),
        AiEvaluationConfigError::InvalidIdentifier
    );
}

#[test]
fn suite_limit_stops_before_consuming_the_remaining_iterator() {
    let cases = (0..10_001)
        .map(|index| {
            AiEvaluationCase::new(
                format!("case-{index}"),
                request("private evaluation prompt"),
                (),
            )
            .unwrap()
        })
        .chain(std::iter::once_with(|| {
            panic!("suite limit must reject before reading the iterator tail")
        }));

    let error = AiEvaluationSuite::new("support.v1", cases).unwrap_err();
    assert_eq!(error, AiEvaluationConfigError::TooManyCases);
}

#[test]
fn evaluation_reference_debug_output_redacts_durable_identifiers() {
    let reference =
        AiEvaluationReference::new("tenant-finance", "restricted-catalog", "run-secret-7").unwrap();

    let output = format!("{reference:?}");

    assert!(!output.contains(reference.scope()));
    assert!(!output.contains(reference.catalog_id()));
    assert!(!output.contains(reference.run_key()));
}

#[test]
fn evaluation_case_debug_output_redacts_request_and_target_content() {
    let case = AiEvaluationCase::new(
        "case-private",
        request("private evaluation prompt"),
        "private grading target".to_owned(),
    )
    .unwrap();

    let output = format!("{case:?}");

    assert!(output.contains("case-private"));
    assert!(output.contains("[REDACTED]"));
    assert!(!output.contains("private evaluation prompt"));
    assert!(!output.contains("private grading target"));
}
