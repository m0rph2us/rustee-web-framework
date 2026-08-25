//! Shared deterministic evaluation fixtures and intentionally leaky diagnostic source.

use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_ai::{ChatMessage, ChatRequest, ChatResponse, MessageRole, Usage};

use crate::{
    AiEvaluationCase, AiEvaluationCatalog, AiEvaluationGrade, AiEvaluationGrader,
    AiEvaluationOutcome, AiEvaluationReference, AiEvaluationSuite,
};

pub(super) struct LeakyDiagnosticError;

impl fmt::Debug for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticError(private-evaluation-prompt)")
    }
}

impl fmt::Display for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-evaluation-prompt")
    }
}

impl std::error::Error for LeakyDiagnosticError {}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExactTextGrader;

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

pub(super) fn request(content: &str) -> ChatRequest {
    ChatRequest::new(
        "evaluation-model",
        [ChatMessage::new(MessageRole::User, content).unwrap()],
    )
    .unwrap()
}

pub(super) fn response(
    id: &str,
    content: &str,
    input_tokens: u64,
    output_tokens: u64,
) -> ChatResponse {
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
pub(super) struct Catalog {
    pub(super) loads: Arc<Mutex<usize>>,
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
