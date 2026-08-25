//! Stable facade for evaluation suite definitions and grading contracts.

mod definition;
mod grading;

pub use definition::{
    AiEvaluationCase, AiEvaluationConfigError, AiEvaluationReference, AiEvaluationSuite,
    MAX_EVALUATION_IDENTIFIER_BYTES,
};
pub use grading::{AiEvaluationGrade, AiEvaluationGrader, AiEvaluationOutcome};

pub(crate) const MAX_SCORE_PER_MILLE: u16 = grading::MAX_SCORE_PER_MILLE;
