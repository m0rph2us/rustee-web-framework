use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use rustee_ai::{AiPipeline, AiProvider, ChatRequest, ChatResponse, PipelineError, Usage};

use crate::{
    AiEvaluationGrade, AiEvaluationGrader, AiEvaluationOutcome, AiEvaluationSuite,
    model::MAX_SCORE_PER_MILLE,
};

/// Application-selected path for one model completion inside an evaluation run.
///
/// Implement this trait around the application's already-governed pipeline. That preserves its
/// budget, usage-ledger, authorization, and retry policy instead of allowing evaluation to make a
/// hidden provider call path.
pub trait AiEvaluationExecutor: Clone + Send + Sync + 'static {
    /// Application-specific completion failure.
    type Error: StdError + Send + Sync + 'static;

    /// Completes one evaluation request without retries.
    fn complete_for_evaluation(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>>;
}

impl<P> AiEvaluationExecutor for AiPipeline<P>
where
    P: AiProvider,
{
    type Error = PipelineError<P::Error>;

    fn complete_for_evaluation(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        let pipeline = self.clone();
        Box::pin(async move { pipeline.complete(request).await })
    }
}

/// One content-free completed case result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiEvaluationCaseResult {
    case_id: String,
    model: String,
    usage: Usage,
    grade: AiEvaluationGrade,
}

impl AiEvaluationCaseResult {
    /// Returns the stable case identifier.
    #[must_use]
    pub fn case_id(&self) -> &str {
        &self.case_id
    }

    /// Returns the provider-resolved model alias without response content.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns provider-reported usage for this case.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }

    /// Returns the trusted grader's normalized content-free grade.
    #[must_use]
    pub const fn grade(&self) -> &AiEvaluationGrade {
        &self.grade
    }
}

/// Aggregate content-free statistics for an evaluation report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AiEvaluationSummary {
    total_cases: usize,
    passed_cases: usize,
    failed_cases: usize,
    average_score_per_mille: u16,
    usage: Usage,
}

impl AiEvaluationSummary {
    /// Returns the number of fully completed cases.
    #[must_use]
    pub const fn total_cases(&self) -> usize {
        self.total_cases
    }

    /// Returns the number of accepted case grades.
    #[must_use]
    pub const fn passed_cases(&self) -> usize {
        self.passed_cases
    }

    /// Returns the number of rejected case grades.
    #[must_use]
    pub const fn failed_cases(&self) -> usize {
        self.failed_cases
    }

    /// Returns the floor average of per-case score on the zero-to-1,000 scale.
    #[must_use]
    pub const fn average_score_per_mille(&self) -> u16 {
        self.average_score_per_mille
    }

    /// Returns total provider-reported usage across completed cases.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }
}

/// Report produced only after every case in one suite completed and graded successfully.
pub struct AiEvaluationReport {
    suite_name: String,
    cases: Vec<AiEvaluationCaseResult>,
    summary: AiEvaluationSummary,
}

impl AiEvaluationReport {
    /// Returns the stable application-selected suite name.
    #[must_use]
    pub fn suite_name(&self) -> &str {
        &self.suite_name
    }

    /// Returns completed case results in explicit suite order.
    #[must_use]
    pub fn cases(&self) -> &[AiEvaluationCaseResult] {
        &self.cases
    }

    /// Returns aggregate content-free run statistics.
    #[must_use]
    pub const fn summary(&self) -> &AiEvaluationSummary {
        &self.summary
    }
}

impl fmt::Debug for AiEvaluationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationReport")
            .field("suite_name", &self.suite_name)
            .field("cases", &self.cases)
            .field("summary", &self.summary)
            .finish()
    }
}

/// Fail-fast runner for one bounded evaluation suite.
#[derive(Clone)]
pub struct AiEvaluationRunner<E, G> {
    executor: E,
    grader: G,
}

impl<E, G> fmt::Debug for AiEvaluationRunner<E, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationRunner")
            .field("executor_type", &std::any::type_name::<E>())
            .field("grader_type", &std::any::type_name::<G>())
            .finish_non_exhaustive()
    }
}

impl<E, G> AiEvaluationRunner<E, G> {
    /// Creates a runner from an application-governed executor and trusted grader.
    #[must_use]
    pub const fn new(executor: E, grader: G) -> Self {
        Self { executor, grader }
    }
}

impl<E, G> AiEvaluationRunner<E, G>
where
    E: AiEvaluationExecutor,
{
    /// Runs one suite in explicit case order without automatic retry or parallel fan-out.
    ///
    /// A completion or grading failure ends the run before later cases start. The caller decides
    /// whether a fresh run, a durable batch reference, or a human review is appropriate.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationRunError`] when a single executor or grader operation fails.
    pub async fn run<T>(
        &self,
        suite: &AiEvaluationSuite<T>,
    ) -> Result<AiEvaluationReport, AiEvaluationRunError<E::Error, G::Error>>
    where
        G: AiEvaluationGrader<T>,
        T: Send + Sync + 'static,
    {
        let mut cases = Vec::with_capacity(suite.cases().len());
        for case in suite.cases() {
            let response = self
                .executor
                .complete_for_evaluation(case.request().clone())
                .await
                .map_err(|source| AiEvaluationRunError::Executor {
                    case_id: case.id().to_owned(),
                    source,
                })?;
            let grade = self.grader.grade(case, &response).await.map_err(|source| {
                AiEvaluationRunError::Grader {
                    case_id: case.id().to_owned(),
                    source,
                }
            })?;
            cases.push(AiEvaluationCaseResult {
                case_id: case.id().to_owned(),
                model: response.model().to_owned(),
                usage: response.usage(),
                grade,
            });
        }
        Ok(AiEvaluationReport {
            suite_name: suite.name().to_owned(),
            summary: summary(&cases),
            cases,
        })
    }
}

/// One sanitized failure during a fail-fast evaluation run.
///
/// Its display and debug forms retain only the failure category and stable case ID. The underlying
/// application error remains available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum AiEvaluationRunError<ExecutorError, GraderError> {
    /// The application-governed executor could not complete one case. Later cases did not run.
    #[error("AI evaluation executor failed for one case")]
    Executor {
        /// Stable safe case ID for application retry or triage routing.
        case_id: String,
        /// The application executor failure.
        #[source]
        source: ExecutorError,
    },
    /// The trusted application grader failed for one completed response. Later cases did not run.
    #[error("AI evaluation grader failed for one case")]
    Grader {
        /// Stable safe case ID for application triage routing.
        case_id: String,
        /// The application grader failure.
        #[source]
        source: GraderError,
    },
}

impl<ExecutorError, GraderError> fmt::Debug for AiEvaluationRunError<ExecutorError, GraderError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Executor { case_id, .. } => formatter
                .debug_struct("AiEvaluationRunError::Executor")
                .field("case_id", case_id)
                .finish(),
            Self::Grader { case_id, .. } => formatter
                .debug_struct("AiEvaluationRunError::Grader")
                .field("case_id", case_id)
                .finish(),
        }
    }
}

impl<ExecutorError, GraderError> AiEvaluationRunError<ExecutorError, GraderError> {
    /// Returns the stable case identifier for the failed operation.
    #[must_use]
    pub fn case_id(&self) -> &str {
        match self {
            Self::Executor { case_id, .. } | Self::Grader { case_id, .. } => case_id,
        }
    }
}

fn summary(cases: &[AiEvaluationCaseResult]) -> AiEvaluationSummary {
    let total_cases = cases.len();
    let passed_cases = cases
        .iter()
        .filter(|case| case.grade().outcome() == AiEvaluationOutcome::Passed)
        .count();
    let score_sum = cases
        .iter()
        .map(|case| u64::from(case.grade().score_per_mille()))
        .sum::<u64>();
    let input_tokens = cases.iter().fold(0_u64, |total, case| {
        total.saturating_add(case.usage().input_tokens)
    });
    let output_tokens = cases.iter().fold(0_u64, |total, case| {
        total.saturating_add(case.usage().output_tokens)
    });
    AiEvaluationSummary {
        total_cases,
        passed_cases,
        failed_cases: total_cases - passed_cases,
        average_score_per_mille: u16::try_from(score_sum / total_cases as u64)
            .unwrap_or(MAX_SCORE_PER_MILLE),
        usage: Usage {
            input_tokens,
            output_tokens,
        },
    }
}
