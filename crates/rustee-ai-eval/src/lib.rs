//! Provider-neutral, bounded AI evaluation suites.
//!
//! Evaluation prompts, expected answers, and model completions are application-owned sensitive
//! data. This crate keeps them in memory only while a trusted application executor and grader run
//! a suite. Its resulting report carries safe case identifiers, normalized grades, model aliases,
//! and aggregate usage, never request, expected-answer, or completion text. It neither retries a
//! failed case nor serializes a suite into a durable job payload.

mod in_memory;
mod model;
mod runner;
mod submission;

pub use in_memory::{InMemoryAiEvaluationRunLedger, InMemoryAiEvaluationRunLedgerError};
pub use model::{
    AiEvaluationCase, AiEvaluationConfigError, AiEvaluationGrade, AiEvaluationGrader,
    AiEvaluationOutcome, AiEvaluationReference, AiEvaluationSuite, MAX_EVALUATION_IDENTIFIER_BYTES,
};
pub use runner::{
    AiEvaluationCaseResult, AiEvaluationExecutor, AiEvaluationReport, AiEvaluationRunError,
    AiEvaluationRunner, AiEvaluationSummary,
};
pub use submission::{
    AiEvaluationCatalog, AiEvaluationRunLedger, AiEvaluationRunReservation, AiEvaluationSubmission,
    AiEvaluationSubmissionError, AiEvaluationSubmitter,
};

#[cfg(test)]
mod tests;
