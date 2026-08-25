use std::sync::{Arc, Mutex};

use rustee_ai::AiPipeline;
use rustee_ai_test::{RecordedAiError, RecordedAiProvider};

use super::{
    AiEvaluationCase, AiEvaluationConfigError, AiEvaluationOutcome, AiEvaluationReference,
    AiEvaluationRunError, AiEvaluationRunner, AiEvaluationSubmission, AiEvaluationSubmissionError,
    AiEvaluationSubmitter, AiEvaluationSuite, InMemoryAiEvaluationRunLedger,
    MAX_EVALUATION_IDENTIFIER_BYTES,
};

mod model;
mod runner;
mod submission;
mod support;

use support::{Catalog, ExactTextGrader, LeakyDiagnosticError, request, response};
