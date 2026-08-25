//! Evaluation runner execution, summary, and diagnostic regression coverage.

use super::*;

#[test]
fn evaluation_runner_debug_does_not_delegate_to_executor_or_grader_diagnostics() {
    let runner = AiEvaluationRunner::new(LeakyDiagnosticError, LeakyDiagnosticError);

    let debug = format!("{runner:?}");

    assert!(debug.contains("executor_type"));
    assert!(debug.contains("grader_type"));
    assert!(!debug.contains("private-evaluation-prompt"));
}

#[test]
fn evaluation_error_diagnostics_redact_adapter_details_and_preserve_source_chains() {
    let runner_error =
        AiEvaluationRunError::<LeakyDiagnosticError, LeakyDiagnosticError>::Executor {
            case_id: "case-a".to_owned(),
            source: LeakyDiagnosticError,
        };
    let submission_error = AiEvaluationSubmissionError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Run {
        source: runner_error,
    };
    let grader_error = AiEvaluationRunError::<LeakyDiagnosticError, LeakyDiagnosticError>::Grader {
        case_id: "case-b".to_owned(),
        source: LeakyDiagnosticError,
    };

    for error in [
        &submission_error as &dyn std::error::Error,
        &grader_error as &dyn std::error::Error,
    ] {
        assert!(!format!("{error:?}").contains("private-evaluation-prompt"));
        assert!(!error.to_string().contains("private-evaluation-prompt"));
    }
    let runner_source = std::error::Error::source(&submission_error)
        .expect("submission error must preserve its runner source");
    assert!(std::error::Error::source(runner_source).is_some());
}

#[tokio::test]
async fn runner_reports_content_free_results_and_aggregated_usage() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("provider-1", "expected one", 3, 5));
    provider.queue_completion(response("provider-2", "wrong", 7, 11));
    let suite = AiEvaluationSuite::new(
        "support.v1",
        [
            AiEvaluationCase::new(
                "answer.1",
                request("private prompt one"),
                "expected one".to_owned(),
            )
            .unwrap(),
            AiEvaluationCase::new(
                "answer.2",
                request("private prompt two"),
                "expected two".to_owned(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let report = AiEvaluationRunner::new(AiPipeline::new(provider), ExactTextGrader)
        .run(&suite)
        .await
        .unwrap();

    assert_eq!(report.suite_name(), "support.v1");
    assert_eq!(report.summary().total_cases(), 2);
    assert_eq!(report.summary().passed_cases(), 1);
    assert_eq!(report.summary().failed_cases(), 1);
    assert_eq!(report.summary().average_score_per_mille(), 500);
    assert_eq!(report.summary().usage().input_tokens, 10);
    assert_eq!(report.summary().usage().output_tokens, 16);
    assert_eq!(report.cases()[0].case_id(), "answer.1");
    assert_eq!(
        report.cases()[1].grade().outcome(),
        AiEvaluationOutcome::Failed
    );
    let debug = format!("{report:?}");
    assert!(!debug.contains("private prompt"));
    assert!(!debug.contains("expected one"));
    assert!(!debug.contains("wrong"));
}

#[tokio::test]
async fn runner_saturates_aggregate_usage_from_provider_reports() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("provider-1", "expected one", u64::MAX, u64::MAX));
    provider.queue_completion(response("provider-2", "expected two", 1, 1));
    let suite = AiEvaluationSuite::new(
        "support.v1",
        [
            AiEvaluationCase::new(
                "answer.1",
                request("private prompt one"),
                "expected one".to_owned(),
            )
            .unwrap(),
            AiEvaluationCase::new(
                "answer.2",
                request("private prompt two"),
                "expected two".to_owned(),
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let report = AiEvaluationRunner::new(AiPipeline::new(provider), ExactTextGrader)
        .run(&suite)
        .await
        .unwrap();

    assert_eq!(report.summary().usage().input_tokens, u64::MAX);
    assert_eq!(report.summary().usage().output_tokens, u64::MAX);
}

#[tokio::test]
async fn executor_failure_stops_the_suite_without_retrying_or_starting_later_cases() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion_failure(RecordedAiError::Unavailable);
    provider.queue_completion(response("provider-2", "expected two", 1, 1));
    let suite = AiEvaluationSuite::new(
        "support.v1",
        [
            AiEvaluationCase::new("answer.1", request("private one"), "one".to_owned()).unwrap(),
            AiEvaluationCase::new("answer.2", request("private two"), "two".to_owned()).unwrap(),
        ],
    )
    .unwrap();
    let runner = AiEvaluationRunner::new(AiPipeline::new(provider.clone()), ExactTextGrader);

    let error = runner.run(&suite).await.unwrap_err();
    assert_eq!(error.case_id(), "answer.1");
    assert_eq!(provider.recorded_requests().len(), 1);
}
