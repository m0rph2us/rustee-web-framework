//! Evaluation submission and durable reservation regression coverage.

use super::*;

#[tokio::test]
async fn reference_submission_runs_once_and_reuses_a_durable_completion() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("provider-1", "expected", 3, 5));
    let loads = Arc::new(Mutex::new(0));
    let submitter = AiEvaluationSubmitter::new(
        Catalog {
            loads: loads.clone(),
        },
        InMemoryAiEvaluationRunLedger::new(4).unwrap(),
        AiEvaluationRunner::new(AiPipeline::new(provider.clone()), ExactTextGrader),
    );
    let reference = AiEvaluationReference::new("tenant-a.v1", "catalog-7", "run-7").unwrap();

    let first = submitter.submit::<String>(reference.clone()).await.unwrap();
    assert!(matches!(first, AiEvaluationSubmission::Completed(_)));
    let second = submitter.submit::<String>(reference).await.unwrap();
    assert!(matches!(second, AiEvaluationSubmission::ExistingCompletion));
    assert_eq!(*loads.lock().unwrap(), 1);
    assert_eq!(provider.recorded_requests().len(), 1);
}

#[tokio::test]
async fn failed_reference_submission_stays_pending_without_a_second_provider_call() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion_failure(RecordedAiError::Unavailable);
    let loads = Arc::new(Mutex::new(0));
    let submitter = AiEvaluationSubmitter::new(
        Catalog {
            loads: loads.clone(),
        },
        InMemoryAiEvaluationRunLedger::new(4).unwrap(),
        AiEvaluationRunner::new(AiPipeline::new(provider.clone()), ExactTextGrader),
    );
    let reference = AiEvaluationReference::new("tenant-a.v1", "catalog-7", "run-7").unwrap();

    assert!(submitter.submit::<String>(reference.clone()).await.is_err());
    let pending = submitter.submit::<String>(reference).await.unwrap_err();
    assert!(pending.pending_reference().is_some());
    assert_eq!(*loads.lock().unwrap(), 1);
    assert_eq!(provider.recorded_requests().len(), 1);
}
