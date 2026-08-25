//! Content-free grading output and application grader contracts.

use std::error::Error as StdError;

use futures_util::future::BoxFuture;
use rustee_ai::ChatResponse;

use super::definition::{AiEvaluationCase, AiEvaluationConfigError};

const MAX_LABEL_BYTES: usize = 128;
pub(super) const MAX_SCORE_PER_MILLE: u16 = 1_000;

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

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_LABEL_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'"' && byte != b'\\')
}
