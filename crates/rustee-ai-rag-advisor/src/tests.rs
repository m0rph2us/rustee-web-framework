use std::{convert::Infallible, fmt};

use futures_util::{future, future::BoxFuture};
use rustee_ai::{AiAdvisor, ChatMessage, ChatRequest, MessageRole};
use rustee_ai_rag::{
    Citation, RagError, RetrievalContext, RetrievalQuery, RetrievalScope, RetrievalStore,
    RetrievedChunk,
};

use super::{RagAdvisor, RagAdvisorError, RagContextRenderer, RagQueryBuilder};

struct LeakyDiagnosticError;

impl fmt::Debug for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticError(private-retrieval-query)")
    }
}

impl fmt::Display for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-retrieval-query")
    }
}

impl std::error::Error for LeakyDiagnosticError {}

#[derive(Clone)]
struct FakeStore {
    chunks: Vec<RetrievedChunk>,
}

impl RetrievalStore for FakeStore {
    type Error = Infallible;

    fn search(
        &self,
        _: RetrievalQuery,
    ) -> BoxFuture<'static, Result<Vec<RetrievedChunk>, Self::Error>> {
        Box::pin(future::ready(Ok(self.chunks.clone())))
    }
}

#[derive(Clone, Copy)]
struct FixedQuery;

impl RagQueryBuilder for FixedQuery {
    type Error = Infallible;

    fn build(&self, request: &ChatRequest) -> Result<RetrievalQuery, Self::Error> {
        assert_eq!(request.messages()[0].content(), "What is the policy?");
        Ok(RetrievalQuery::new(
            "policy",
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            2,
        )
        .unwrap())
    }
}

#[derive(Clone, Copy)]
struct ExplicitRenderer;

impl RagContextRenderer for ExplicitRenderer {
    type Error = Infallible;

    fn render(&self, context: &RetrievalContext) -> Result<ChatMessage, Self::Error> {
        let source = context.chunks()[0].citation().source_uri();
        Ok(ChatMessage::new(
            MessageRole::System,
            format!("Untrusted retrieval source {source}: account policy text"),
        )
        .expect("rendered test context is non-blank"))
    }
}

fn request() -> ChatRequest {
    ChatRequest::new(
        "support.default",
        [ChatMessage::new(MessageRole::User, "What is the policy?").unwrap()],
    )
    .unwrap()
}

fn chunk(tenant: &str) -> RetrievedChunk {
    RetrievedChunk::new(
        "chunk-1",
        "doc-1",
        tenant,
        "account policy text",
        Citation::new("kb://policy", "Account policy").unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn advisor_adds_only_application_rendered_revalidated_context() {
    let advisor = RagAdvisor::new(
        FakeStore {
            chunks: vec![chunk("acme")],
        },
        FixedQuery,
        ExplicitRenderer,
    );

    let request = advisor.before_request(request()).await.unwrap();

    assert_eq!(request.messages().len(), 2);
    assert_eq!(request.messages()[1].role(), MessageRole::System);
    assert_eq!(
        request.messages()[1].content(),
        "Untrusted retrieval source kb://policy: account policy text"
    );
    assert!(!format!("{advisor:?}").contains("account policy text"));
}

#[tokio::test]
async fn advisor_fails_closed_before_prompt_rendering_for_scope_violation() {
    let advisor = RagAdvisor::new(
        FakeStore {
            chunks: vec![chunk("other-tenant")],
        },
        FixedQuery,
        ExplicitRenderer,
    );

    let error = advisor.before_request(request()).await.unwrap_err();

    assert!(matches!(
        error,
        RagAdvisorError::Retrieval(RagError::ScopeViolation)
    ));
}

#[test]
fn advisor_error_diagnostics_redact_application_details_and_preserve_sources() {
    let query =
        RagAdvisorError::<LeakyDiagnosticError, LeakyDiagnosticError, LeakyDiagnosticError>::Query(
            LeakyDiagnosticError,
        );
    let render =
        RagAdvisorError::<LeakyDiagnosticError, LeakyDiagnosticError, LeakyDiagnosticError>::Render(
            LeakyDiagnosticError,
        );

    for error in [
        &query as &dyn std::error::Error,
        &render as &dyn std::error::Error,
    ] {
        assert!(!format!("{error:?}").contains("private-retrieval-query"));
        assert!(!error.to_string().contains("private-retrieval-query"));
        assert!(std::error::Error::source(error).is_some());
    }
}
