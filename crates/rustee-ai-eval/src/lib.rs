//! Provider-neutral, bounded AI evaluation suites.
//!
//! Evaluation prompts, expected answers, and model completions are application-owned sensitive
//! data. This crate keeps them in memory only while a trusted application executor and grader run
//! a suite. Its resulting report carries safe case identifiers, normalized grades, model aliases,
//! and aggregate usage, never request, expected-answer, or completion text. It neither retries a
//! failed case nor serializes a suite into a durable job payload.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_ai::{AiPipeline, AiProvider, ChatRequest, ChatResponse, PipelineError, Usage};
use serde::{Deserialize, Serialize};

const MAX_CASES_PER_SUITE: usize = 10_000;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_LABEL_BYTES: usize = 128;
const MAX_SCORE_PER_MILLE: u16 = 1_000;
const MAX_IN_MEMORY_RUNS: usize = 10_000;

/// Content-free reference to an application-owned durable AI evaluation catalog entry.
///
/// `scope` is an opaque tenant/policy boundary, `catalog_id` selects application-owned prompts
/// and grading targets after authorization, and `run_key` is the stable durable delivery key.
/// None of these fields may contain request, target, completion, or grader content.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiEvaluationReference {
    scope: String,
    catalog_id: String,
    run_key: String,
}

impl<'de> Deserialize<'de> for AiEvaluationReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiEvaluationReference {
            scope: String,
            catalog_id: String,
            run_key: String,
        }

        let raw = RawAiEvaluationReference::deserialize(deserializer)?;
        Self::new(raw.scope, raw.catalog_id, raw.run_key).map_err(serde::de::Error::custom)
    }
}

impl AiEvaluationReference {
    /// Creates a bounded reference to one application catalog entry and durable evaluation run.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidIdentifier`] for unsafe identifiers.
    pub fn new(
        scope: impl Into<String>,
        catalog_id: impl Into<String>,
        run_key: impl Into<String>,
    ) -> Result<Self, AiEvaluationConfigError> {
        let reference = Self {
            scope: scope.into(),
            catalog_id: catalog_id.into(),
            run_key: run_key.into(),
        };
        if !valid_identifier(&reference.scope)
            || !valid_identifier(&reference.catalog_id)
            || !valid_identifier(&reference.run_key)
        {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the application-owned tenant/policy isolation scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Returns the application catalog identifier without loading its contents.
    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    /// Returns the stable idempotency key for the durable evaluation run.
    #[must_use]
    pub fn run_key(&self) -> &str {
        &self.run_key
    }
}

/// One application-owned test case in an AI evaluation suite.
///
/// `target` is intentionally generic. A trusted application grader defines its format, such as an
/// expected structured DTO, a fixed answer, a reference document ID, or a human-label reference.
/// Rustee does not serialize, log, or expose it from an evaluation report.
pub struct AiEvaluationCase<T> {
    id: String,
    request: ChatRequest,
    target: T,
}

impl<T> AiEvaluationCase<T> {
    /// Creates one bounded, stable case ID with a request and application-owned grading target.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidIdentifier`] when `id` is not a bounded stable
    /// ASCII identifier.
    pub fn new(
        id: impl Into<String>,
        request: ChatRequest,
        target: T,
    ) -> Result<Self, AiEvaluationConfigError> {
        let id = id.into();
        if !valid_identifier(&id) {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        Ok(Self {
            id,
            request,
            target,
        })
    }

    /// Returns the stable application-selected case ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the evaluation request. Do not log this value by default.
    #[must_use]
    pub const fn request(&self) -> &ChatRequest {
        &self.request
    }

    /// Returns the trusted application-owned grading target. Do not log this value by default.
    #[must_use]
    pub const fn target(&self) -> &T {
        &self.target
    }
}

impl<T> fmt::Debug for AiEvaluationCase<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationCase")
            .field("id", &self.id)
            .field("request", &self.request)
            .field("target", &"[APPLICATION-OWNED]")
            .finish()
    }
}

/// A bounded, ordered set of application-owned evaluation cases.
pub struct AiEvaluationSuite<T> {
    name: String,
    cases: Vec<AiEvaluationCase<T>>,
}

impl<T> AiEvaluationSuite<T> {
    /// Creates a named evaluation suite with unique case identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError`] for an invalid suite name, empty/oversized suite, or a
    /// duplicated case ID. Case order is preserved and execution remains sequential.
    pub fn new(
        name: impl Into<String>,
        cases: impl IntoIterator<Item = AiEvaluationCase<T>>,
    ) -> Result<Self, AiEvaluationConfigError> {
        let name = name.into();
        if !valid_identifier(&name) {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        let cases = cases.into_iter().collect::<Vec<_>>();
        if cases.is_empty() {
            return Err(AiEvaluationConfigError::EmptySuite);
        }
        if cases.len() > MAX_CASES_PER_SUITE {
            return Err(AiEvaluationConfigError::TooManyCases);
        }
        let mut ids = cases.iter().map(AiEvaluationCase::id).collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AiEvaluationConfigError::DuplicateCaseIdentifier);
        }
        Ok(Self { name, cases })
    }

    /// Returns the stable application-selected suite name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns cases in the explicit evaluation order.
    #[must_use]
    pub fn cases(&self) -> &[AiEvaluationCase<T>] {
        &self.cases
    }
}

impl<T> fmt::Debug for AiEvaluationSuite<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationSuite")
            .field("name", &self.name)
            .field("case_count", &self.cases.len())
            .finish()
    }
}

/// Application-owned authorized loader for one evaluation catalog entry.
///
/// The returned suite may contain raw prompts, expected targets, or private grader metadata. It
/// stays in the trusted worker process and is never serialized by this crate's reference or run
/// ledger APIs.
pub trait AiEvaluationCatalog<T>: Clone + Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    /// Catalog lookup or authorization failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads the exact tenant-scoped evaluation suite after durable dispatch begins.
    fn load(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationSuite<T>, Self::Error>>;
}

/// Invalid public evaluation-suite or grade configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiEvaluationConfigError {
    /// Suite names and case IDs are intentionally bounded opaque identifiers.
    #[error(
        "AI evaluation identifier must use bounded ASCII letters, digits, underscore, hyphen, or dot"
    )]
    InvalidIdentifier,
    /// An evaluation suite needs an explicit least one case.
    #[error("AI evaluation suite must contain at least one case")]
    EmptySuite,
    /// Evaluation execution intentionally has a finite in-memory bound.
    #[error("AI evaluation suite supports at most 10,000 cases")]
    TooManyCases,
    /// A suite cannot report two different results under one case ID.
    #[error("AI evaluation suite contains a duplicate case identifier")]
    DuplicateCaseIdentifier,
    /// A grader label was malformed or unsafe for a content-free report.
    #[error(
        "AI evaluation grade label must use bounded visible ASCII without whitespace, quotes, or backslashes"
    )]
    InvalidGradeLabel,
    /// Grading scores use an integer per-mille scale to avoid unbounded float behavior.
    #[error("AI evaluation grade must not exceed 1,000 per mille")]
    ScoreOutOfRange,
    /// The in-memory development ledger has no safe fixed capacity.
    #[error("AI in-memory evaluation ledger capacity must be between one and 10,000 records")]
    InvalidInMemoryLedgerCapacity,
}

/// The normalized pass/fail outcome returned by a trusted application grader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiEvaluationOutcome {
    /// The grader accepted the response for this case.
    Passed,
    /// The grader rejected the response for this case.
    Failed,
}

/// A bounded, content-free grade for one model response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiEvaluationGrade {
    outcome: AiEvaluationOutcome,
    score_per_mille: u16,
    label: String,
}

impl AiEvaluationGrade {
    /// Creates a normalized grade from a pass/fail result, score, and application-defined label.
    ///
    /// Labels are safe categories such as `exact-match`, `schema-valid`, or `human-approved`;
    /// they must not contain snippets of the prompt, expected answer, or completion.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError`] when the score or label is invalid.
    pub fn new(
        outcome: AiEvaluationOutcome,
        score_per_mille: u16,
        label: impl Into<String>,
    ) -> Result<Self, AiEvaluationConfigError> {
        let label = label.into();
        if score_per_mille > MAX_SCORE_PER_MILLE {
            return Err(AiEvaluationConfigError::ScoreOutOfRange);
        }
        if !valid_label(&label) {
            return Err(AiEvaluationConfigError::InvalidGradeLabel);
        }
        Ok(Self {
            outcome,
            score_per_mille,
            label,
        })
    }

    /// Returns the grader's pass/fail outcome.
    #[must_use]
    pub const fn outcome(&self) -> AiEvaluationOutcome {
        self.outcome
    }

    /// Returns the integer score on a zero-to-1,000 per-mille scale.
    #[must_use]
    pub const fn score_per_mille(&self) -> u16 {
        self.score_per_mille
    }

    /// Returns the safe application-defined grading category.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }
}

/// Application-owned grader for one evaluation target type.
///
/// The grader can inspect the raw evaluation request, target, and response, but must return only
/// a content-free [`AiEvaluationGrade`]. Implementations must not place prompt, target, or model
/// completion snippets in the grade label or error display text.
pub trait AiEvaluationGrader<T>: Clone + Send + Sync + 'static {
    /// Application-specific grading failure.
    type Error: StdError + Send + Sync + 'static;

    /// Grades one completed response against its trusted application case.
    fn grade<'a>(
        &'a self,
        case: &'a AiEvaluationCase<T>,
        response: &'a ChatResponse,
    ) -> BoxFuture<'a, Result<AiEvaluationGrade, Self::Error>>;
}

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

/// State returned when one scoped evaluation run is reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiEvaluationRunReservation {
    /// This caller owns the first evaluation attempt for the exact reference.
    Reserved,
    /// A prior evaluation completed and must not be rerun automatically.
    Completed,
    /// A prior attempt may have loaded a catalog, called a provider, or graded a result without a
    /// durable completion record.
    Pending,
}

/// Application-owned durable idempotency boundary for one evaluation run.
///
/// A production ledger must atomically reserve a scoped run key and survive worker restarts.
/// There is intentionally no release-and-retry operation: failures after reservation remain
/// pending until application owners review provider usage, grading policy, and any report sink.
pub trait AiEvaluationRunLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one run, returns a prior completion, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationRunReservation, Self::Error>>;

    /// Records that the application completed and accepted the evaluation run.
    fn record_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Fail-fast runner for one bounded evaluation suite.
#[derive(Clone, Debug)]
pub struct AiEvaluationRunner<E, G> {
    executor: E,
    grader: G,
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
#[derive(Debug, thiserror::Error)]
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

impl<ExecutorError, GraderError> AiEvaluationRunError<ExecutorError, GraderError> {
    /// Returns the stable case identifier for the failed operation.
    #[must_use]
    pub fn case_id(&self) -> &str {
        match self {
            Self::Executor { case_id, .. } | Self::Grader { case_id, .. } => case_id,
        }
    }
}

/// Result of one reference-backed evaluation submission.
pub enum AiEvaluationSubmission {
    /// This call loaded a catalog, ran every case, and durably recorded completion.
    Completed(AiEvaluationReport),
    /// A prior durable completion blocked catalog loading and model execution.
    ExistingCompletion,
}

impl fmt::Debug for AiEvaluationSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(report) => formatter.debug_tuple("Completed").field(report).finish(),
            Self::ExistingCompletion => formatter.write_str("ExistingCompletion"),
        }
    }
}

/// Coordinator for catalog loading, atomic run reservation, fail-fast evaluation, and completion.
#[derive(Clone)]
pub struct AiEvaluationSubmitter<C, L, E, G> {
    catalog: C,
    ledger: L,
    runner: AiEvaluationRunner<E, G>,
}

impl<C, L, E, G> AiEvaluationSubmitter<C, L, E, G> {
    /// Creates a reference-backed coordinator from explicit application boundaries.
    #[must_use]
    pub const fn new(catalog: C, ledger: L, runner: AiEvaluationRunner<E, G>) -> Self {
        Self {
            catalog,
            ledger,
            runner,
        }
    }
}

impl<C, L, E, G> fmt::Debug for AiEvaluationSubmitter<C, L, E, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationSubmitter")
            .field("catalog", &"[APPLICATION-OWNED]")
            .field("ledger", &"[APPLICATION-OWNED]")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<C, L, E, G> AiEvaluationSubmitter<C, L, E, G>
where
    L: AiEvaluationRunLedger,
    E: AiEvaluationExecutor,
{
    /// Runs one application catalog entry at most once for its stable scoped run key.
    ///
    /// # Errors
    ///
    /// A catalog, executor, grader, or completion-record failure leaves the reservation pending.
    /// The coordinator never reloads the suite or makes another provider call automatically;
    /// application owners must reconcile and choose a new explicit run key when appropriate.
    pub async fn submit<T>(
        &self,
        reference: AiEvaluationReference,
    ) -> Result<
        AiEvaluationSubmission,
        AiEvaluationSubmissionError<C::Error, L::Error, E::Error, G::Error>,
    >
    where
        C: AiEvaluationCatalog<T>,
        G: AiEvaluationGrader<T>,
        T: Send + Sync + 'static,
    {
        match self
            .ledger
            .reserve(reference.clone())
            .await
            .map_err(|source| AiEvaluationSubmissionError::LedgerReserve { source })?
        {
            AiEvaluationRunReservation::Completed => {
                return Ok(AiEvaluationSubmission::ExistingCompletion);
            }
            AiEvaluationRunReservation::Pending => {
                return Err(AiEvaluationSubmissionError::Pending { reference });
            }
            AiEvaluationRunReservation::Reserved => {}
        }

        let suite = self
            .catalog
            .load(reference.clone())
            .await
            .map_err(|source| AiEvaluationSubmissionError::Catalog { source })?;
        let report = self
            .runner
            .run(&suite)
            .await
            .map_err(|source| AiEvaluationSubmissionError::Run { source })?;
        self.ledger
            .record_completed(reference)
            .await
            .map_err(|source| AiEvaluationSubmissionError::LedgerRecord { source })?;
        Ok(AiEvaluationSubmission::Completed(report))
    }
}

/// One sanitized reference-backed evaluation failure.
#[derive(Debug, thiserror::Error)]
pub enum AiEvaluationSubmissionError<CatalogError, LedgerError, ExecutorError, GraderError> {
    /// Atomic run reservation failed before catalog loading or model execution.
    #[error("AI evaluation run ledger reservation failed")]
    LedgerReserve {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
    /// A prior attempt is ambiguous and must not be rerun automatically.
    #[error("AI evaluation run is pending reconciliation")]
    Pending {
        /// Content-free reference that requires application reconciliation.
        reference: AiEvaluationReference,
    },
    /// The application catalog failed after reservation and before model execution.
    #[error("AI evaluation catalog load failed")]
    Catalog {
        /// Application catalog failure.
        #[source]
        source: CatalogError,
    },
    /// The executor or trusted grader failed during the sequential evaluation run.
    #[error("AI evaluation run failed")]
    Run {
        /// Sanitized runner failure.
        #[source]
        source: AiEvaluationRunError<ExecutorError, GraderError>,
    },
    /// The run completed but its durable completion record did not persist.
    #[error("AI evaluation completion recording failed")]
    LedgerRecord {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
}

impl<CatalogError, LedgerError, ExecutorError, GraderError>
    AiEvaluationSubmissionError<CatalogError, LedgerError, ExecutorError, GraderError>
{
    /// Returns the safe reference when an ambiguous prior reservation blocks execution.
    #[must_use]
    pub fn pending_reference(&self) -> Option<&AiEvaluationReference> {
        match self {
            Self::Pending { reference } => Some(reference),
            _ => None,
        }
    }
}

/// Bounded in-memory run ledger for deterministic tests and local development.
///
/// It is not restart-safe. Production applications need a durable atomic ledger and an explicit
/// reconciliation/retention procedure.
#[derive(Clone)]
pub struct InMemoryAiEvaluationRunLedger {
    state: Arc<Mutex<BTreeMap<(String, String), InMemoryEvaluationRunState>>>,
    capacity: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InMemoryEvaluationRunStatus {
    Pending,
    Completed,
}

#[derive(Clone)]
struct InMemoryEvaluationRunState {
    catalog_id: String,
    status: InMemoryEvaluationRunStatus,
}

impl InMemoryAiEvaluationRunLedger {
    /// Creates a ledger with a fixed number of retained scoped run keys.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidInMemoryLedgerCapacity`] outside the documented
    /// local-development bound.
    pub fn new(capacity: usize) -> Result<Self, AiEvaluationConfigError> {
        if !(1..=MAX_IN_MEMORY_RUNS).contains(&capacity) {
            return Err(AiEvaluationConfigError::InvalidInMemoryLedgerCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(BTreeMap::new())),
            capacity,
        })
    }

    /// Returns the fixed number of retained scoped run keys.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl fmt::Debug for InMemoryAiEvaluationRunLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let records = self
            .state
            .lock()
            .map(|state| state.len())
            .unwrap_or_default();
        formatter
            .debug_struct("InMemoryAiEvaluationRunLedger")
            .field("capacity", &self.capacity)
            .field("retained_records", &records)
            .finish()
    }
}

/// In-memory evaluation ledger failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAiEvaluationRunLedgerError {
    /// The fixed local-development capacity was exhausted without implicit eviction.
    #[error("AI in-memory evaluation ledger capacity is exhausted")]
    CapacityExhausted,
    /// A poisoned lock prevents safely reserving or recording a run.
    #[error("AI in-memory evaluation ledger state is unavailable")]
    StateUnavailable,
    /// A scoped run key was reused for a different catalog identity.
    #[error("AI evaluation run key conflicts with an existing catalog identity")]
    IdentityConflict,
    /// Completion recording requires a prior exact reservation.
    #[error("AI in-memory evaluation ledger has no matching reservation")]
    MissingReservation,
}

impl AiEvaluationRunLedger for InMemoryAiEvaluationRunLedger {
    type Error = InMemoryAiEvaluationRunLedgerError;

    fn reserve(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationRunReservation, Self::Error>> {
        let state = self.state.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiEvaluationRunLedgerError::StateUnavailable)?;
            let key = (reference.scope().to_owned(), reference.run_key().to_owned());
            match state.get(&key) {
                Some(existing) if existing.catalog_id != reference.catalog_id() => {
                    Err(InMemoryAiEvaluationRunLedgerError::IdentityConflict)
                }
                Some(existing) if existing.status == InMemoryEvaluationRunStatus::Pending => {
                    Ok(AiEvaluationRunReservation::Pending)
                }
                Some(_) => Ok(AiEvaluationRunReservation::Completed),
                None => {
                    if state.len() == capacity {
                        return Err(InMemoryAiEvaluationRunLedgerError::CapacityExhausted);
                    }
                    state.insert(
                        key,
                        InMemoryEvaluationRunState {
                            catalog_id: reference.catalog_id().to_owned(),
                            status: InMemoryEvaluationRunStatus::Pending,
                        },
                    );
                    Ok(AiEvaluationRunReservation::Reserved)
                }
            }
        })
    }

    fn record_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiEvaluationRunLedgerError::StateUnavailable)?;
            let key = (reference.scope().to_owned(), reference.run_key().to_owned());
            let existing = state
                .get_mut(&key)
                .ok_or(InMemoryAiEvaluationRunLedgerError::MissingReservation)?;
            if existing.catalog_id != reference.catalog_id() {
                return Err(InMemoryAiEvaluationRunLedgerError::IdentityConflict);
            }
            existing.status = InMemoryEvaluationRunStatus::Completed;
            Ok(())
        })
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
    let input_tokens = cases
        .iter()
        .map(|case| case.usage().input_tokens)
        .sum::<u64>();
    let output_tokens = cases
        .iter()
        .map(|case| case.usage().output_tokens)
        .sum::<u64>();
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

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_LABEL_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'"' && byte != b'\\')
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use futures_util::future::BoxFuture;
    use rustee_ai::{AiPipeline, ChatMessage, ChatRequest, ChatResponse, MessageRole, Usage};
    use rustee_ai_test::{RecordedAiError, RecordedAiProvider};

    use super::{
        AiEvaluationCase, AiEvaluationCatalog, AiEvaluationConfigError, AiEvaluationGrade,
        AiEvaluationGrader, AiEvaluationOutcome, AiEvaluationReference, AiEvaluationRunner,
        AiEvaluationSubmission, AiEvaluationSubmitter, AiEvaluationSuite,
        InMemoryAiEvaluationRunLedger,
    };

    #[derive(Clone, Copy, Debug)]
    struct ExactTextGrader;

    impl AiEvaluationGrader<String> for ExactTextGrader {
        type Error = Infallible;

        fn grade<'a>(
            &'a self,
            case: &'a AiEvaluationCase<String>,
            response: &'a ChatResponse,
        ) -> BoxFuture<'a, Result<AiEvaluationGrade, Self::Error>> {
            let outcome = if response.content() == case.target() {
                AiEvaluationOutcome::Passed
            } else {
                AiEvaluationOutcome::Failed
            };
            Box::pin(async move {
                Ok(AiEvaluationGrade::new(
                    outcome,
                    if outcome == AiEvaluationOutcome::Passed {
                        1_000
                    } else {
                        0
                    },
                    "exact-text",
                )
                .unwrap())
            })
        }
    }

    fn request(content: &str) -> ChatRequest {
        ChatRequest::new(
            "evaluation-model",
            [ChatMessage::new(MessageRole::User, content).unwrap()],
        )
        .unwrap()
    }

    fn response(id: &str, content: &str, input_tokens: u64, output_tokens: u64) -> ChatResponse {
        ChatResponse::new(
            id,
            "provider-model",
            content,
            [],
            Usage {
                input_tokens,
                output_tokens,
            },
        )
        .unwrap()
    }

    #[derive(Clone)]
    struct Catalog {
        loads: Arc<Mutex<usize>>,
    }

    impl AiEvaluationCatalog<String> for Catalog {
        type Error = Infallible;

        fn load(
            &self,
            _reference: AiEvaluationReference,
        ) -> BoxFuture<'static, Result<AiEvaluationSuite<String>, Self::Error>> {
            let loads = self.loads.clone();
            Box::pin(async move {
                *loads.lock().unwrap() += 1;
                Ok(AiEvaluationSuite::new(
                    "catalog-suite.v1",
                    [AiEvaluationCase::new(
                        "case.1",
                        request("private catalog prompt"),
                        "expected".to_owned(),
                    )
                    .unwrap()],
                )
                .unwrap())
            })
        }
    }

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
    async fn executor_failure_stops_the_suite_without_retrying_or_starting_later_cases() {
        let provider = RecordedAiProvider::new();
        provider.queue_completion_failure(RecordedAiError::Unavailable);
        provider.queue_completion(response("provider-2", "expected two", 1, 1));
        let suite = AiEvaluationSuite::new(
            "support.v1",
            [
                AiEvaluationCase::new("answer.1", request("private one"), "one".to_owned())
                    .unwrap(),
                AiEvaluationCase::new("answer.2", request("private two"), "two".to_owned())
                    .unwrap(),
            ],
        )
        .unwrap();
        let runner = AiEvaluationRunner::new(AiPipeline::new(provider.clone()), ExactTextGrader);

        let error = runner.run(&suite).await.unwrap_err();
        assert_eq!(error.case_id(), "answer.1");
        assert_eq!(provider.recorded_requests().len(), 1);
    }

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
}
